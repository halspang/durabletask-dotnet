// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using Microsoft.Extensions.Logging;
using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc;

/// <summary>
/// Represents an independently owned worker connection used for scheduler host fan-out.
/// </summary>
internal interface IGrpcWorkerConnection : IAsyncDisposable
{
    /// <summary>
    /// Runs the connection until it is stopped or its transport needs to be replaced.
    /// </summary>
    /// <param name="onHealthPing">Callback for health pings received on this connection.</param>
    /// <param name="connectionCancellation">Cancellation used to stop only this connection.</param>
    /// <param name="workerCancellation">Cancellation used for work dispatched by this connection.</param>
    /// <returns>A task that completes when the connection stops.</returns>
    Task RunAsync(
        Action<P.HealthPing> onHealthPing,
        CancellationToken connectionCancellation,
        CancellationToken workerCancellation);
}

/// <summary>
/// Reconciles worker connections with the scheduler hosts advertised by health pings.
/// </summary>
internal sealed class GrpcWorkerConnectionManager : IAsyncDisposable
{
    readonly string taskHubName;
    readonly Func<string, IGrpcWorkerConnection> connectionFactory;
    readonly ILogger logger;
    readonly TimeSpan probeTimeout;
    readonly TimeSpan retryDelay;
    readonly TimeSpan deferredDisposeDelay;
    readonly object syncRoot = new();
    readonly HashSet<string> desiredHosts = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<long, ManagedConnection> connections = new();
    readonly HashSet<ManagedConnection> allConnections = new();
    readonly SemaphoreSlim reconcileSignal = new(0);

    CancellationToken workerCancellation;
    CancellationTokenSource? shutdownSource;
    CancellationTokenRegistration workerCancellationRegistration;
    Task? reconcileTask;
    ManagedConnection? activeProbe;
    string? lastProbedHost;
    string? primaryHost;
    long nextConnectionId;
    int reconcilePending;
    int started;
    int disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="GrpcWorkerConnectionManager"/> class.
    /// </summary>
    /// <param name="taskHubName">The task hub whose advertised hosts should be connected.</param>
    /// <param name="connectionFactory">Factory for fresh, independently owned worker connections.</param>
    /// <param name="logger">Logger for connection lifecycle events.</param>
    /// <param name="probeTimeout">Maximum time to wait for a new connection to identify its scheduler host.</param>
    /// <param name="retryDelay">Delay after an unsuccessful host probe.</param>
    /// <param name="deferredDisposeDelay">Grace period before disposing a retired connection.</param>
    public GrpcWorkerConnectionManager(
        string taskHubName,
        Func<string, IGrpcWorkerConnection> connectionFactory,
        ILogger logger,
        TimeSpan probeTimeout,
        TimeSpan retryDelay,
        TimeSpan deferredDisposeDelay)
    {
        this.taskHubName = string.IsNullOrWhiteSpace(taskHubName)
            ? throw new ArgumentException("Task hub name must not be empty.", nameof(taskHubName))
            : taskHubName;
        this.connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.probeTimeout = probeTimeout;
        this.retryDelay = retryDelay;
        this.deferredDisposeDelay = deferredDisposeDelay;
    }

    enum ProbeResult
    {
        Accepted,
        Rejected,
        TimedOut,
        Failed,
    }

    /// <summary>
    /// Gets a snapshot of scheduler hosts with active worker connections.
    /// </summary>
    internal IReadOnlyCollection<string> ConnectedHosts
    {
        get
        {
            lock (this.syncRoot)
            {
                HashSet<string> hosts = new(StringComparer.OrdinalIgnoreCase);
                if (!string.IsNullOrWhiteSpace(this.primaryHost))
                {
                    hosts.Add(this.primaryHost!);
                }

                foreach (ManagedConnection connection in this.connections.Values)
                {
                    hosts.Add(connection.Host);
                }

                return hosts.ToArray();
            }
        }
    }

    /// <summary>
    /// Starts host reconciliation.
    /// </summary>
    /// <param name="workerCancellation">Cancellation token for the owning worker.</param>
    public void Start(CancellationToken workerCancellation)
    {
        if (Interlocked.CompareExchange(ref this.started, 1, 0) != 0)
        {
            throw new InvalidOperationException("The connection manager has already been started.");
        }

#if NET7_0_OR_GREATER
        ObjectDisposedException.ThrowIf(Volatile.Read(ref this.disposed) == 1, this);
#else
        if (Volatile.Read(ref this.disposed) == 1)
        {
            throw new ObjectDisposedException(nameof(GrpcWorkerConnectionManager));
        }
#endif

        this.workerCancellation = workerCancellation;
        this.shutdownSource = new CancellationTokenSource();
        this.workerCancellationRegistration = workerCancellation.Register(
            static state => ((CancellationTokenSource)state!).Cancel(),
            this.shutdownSource);
        this.reconcileTask = this.ReconcileAsync(this.shutdownSource.Token);
        this.SignalReconcile();
    }

    /// <summary>
    /// Reports a health ping from the primary worker connection.
    /// </summary>
    /// <param name="healthPing">The received health ping.</param>
    public void ReportPrimaryHealthPing(P.HealthPing healthPing)
    {
#if NET6_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(healthPing);
#else
        if (healthPing is null)
        {
            throw new ArgumentNullException(nameof(healthPing));
        }
#endif

        if (Volatile.Read(ref this.disposed) == 1 || string.IsNullOrWhiteSpace(healthPing.ConnectedHost))
        {
            return;
        }

        lock (this.syncRoot)
        {
            if (this.disposed == 1)
            {
                return;
            }

            this.primaryHost = healthPing.ConnectedHost;
            this.UpdateDesiredHostsLocked(healthPing);
            this.logger.FanOutTopologyObserved(
                healthPing.ConnectedHost,
                this.taskHubName,
                this.desiredHosts.Count);
        }

        this.SignalReconcile();
    }

    /// <summary>
    /// Clears the scheduler host currently associated with the primary connection.
    /// </summary>
    public void ReportPrimaryDisconnected()
    {
        if (Volatile.Read(ref this.disposed) == 1)
        {
            return;
        }

        lock (this.syncRoot)
        {
            this.primaryHost = null;
        }

        this.SignalReconcile();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref this.disposed, 1) == 1)
        {
            return;
        }

        this.shutdownSource?.Cancel();
        this.workerCancellationRegistration.Dispose();

        if (this.reconcileTask is not null)
        {
            await this.ObserveTaskAsync(
                this.reconcileTask,
                this.shutdownSource?.Token ?? CancellationToken.None).ConfigureAwait(false);
        }

        ManagedConnection[] connections;
        lock (this.syncRoot)
        {
            connections = this.allConnections.ToArray();
        }

        foreach (ManagedConnection connection in connections)
        {
            await this.ObserveTaskAsync(
                connection.RunTask,
                connection.Cancellation).ConfigureAwait(false);
            await this.DisposeConnectionAsync(connection, TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false);
        }

        lock (this.syncRoot)
        {
            this.connections.Clear();
            this.allConnections.Clear();
            this.activeProbe = null;
        }

        this.shutdownSource?.Dispose();
        this.reconcileSignal.Dispose();
    }

    static async Task DelayAsync(TimeSpan delay, CancellationToken cancellation)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, cancellation).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
    }

    static bool IsFatal(Exception exception)
        => exception is OutOfMemoryException
            or StackOverflowException
            or AccessViolationException
            or ThreadAbortException;

    async Task ReconcileAsync(CancellationToken cancellation)
    {
        try
        {
            while (!cancellation.IsCancellationRequested)
            {
                await this.reconcileSignal.WaitAsync(cancellation).ConfigureAwait(false);
                Interlocked.Exchange(ref this.reconcilePending, 0);

                while (!cancellation.IsCancellationRequested)
                {
                    List<ManagedConnection> connectionsToRetire =
                        this.GetConnectionsToRetire(out string? missingHost);
                    foreach (ManagedConnection connection in connectionsToRetire)
                    {
                        this.logger.FanOutConnectionRetired(connection.Host);
                        connection.Cancel();
                    }

                    if (missingHost is null)
                    {
                        break;
                    }

                    ProbeResult result =
                        await this.ProbeOnceAsync(missingHost, cancellation).ConfigureAwait(false);
                    if (result != ProbeResult.Accepted
                        && this.retryDelay > TimeSpan.Zero
                        && this.HasMissingHost())
                    {
                        await Task.Delay(this.retryDelay, cancellation).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
        catch (Exception ex) when (!IsFatal(ex))
        {
            this.logger.FanOutConnectionFailed(ex);
        }
    }

    List<ManagedConnection> GetConnectionsToRetire(out string? missingHost)
    {
        lock (this.syncRoot)
        {
            HashSet<string> occupiedHosts = new(StringComparer.OrdinalIgnoreCase);
            if (!string.IsNullOrWhiteSpace(this.primaryHost))
            {
                occupiedHosts.Add(this.primaryHost!);
            }

            List<ManagedConnection> connectionsToRetire = new();
            foreach (ManagedConnection connection in this.connections.Values.OrderBy(connection => connection.Id))
            {
                if (!this.desiredHosts.Contains(connection.Host) || !occupiedHosts.Add(connection.Host))
                {
                    connectionsToRetire.Add(connection);
                }
            }

            foreach (ManagedConnection connection in connectionsToRetire)
            {
                this.connections.Remove(connection.Id);
            }

            string[] missingHosts = this.desiredHosts
                .Where(host => !occupiedHosts.Contains(host))
                .OrderBy(host => host, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            missingHost = this.SelectNextMissingHostLocked(missingHosts);
            return connectionsToRetire;
        }
    }

    string? SelectNextMissingHostLocked(string[] missingHosts)
    {
        if (missingHosts.Length == 0)
        {
            return null;
        }

        int nextIndex = 0;
        if (this.lastProbedHost is not null)
        {
            int previousIndex = Array.FindIndex(
                missingHosts,
                host => string.Equals(host, this.lastProbedHost, StringComparison.OrdinalIgnoreCase));
            if (previousIndex >= 0)
            {
                nextIndex = (previousIndex + 1) % missingHosts.Length;
            }
            else
            {
                int followingIndex = Array.FindIndex(
                    missingHosts,
                    host => StringComparer.OrdinalIgnoreCase.Compare(host, this.lastProbedHost) > 0);
                if (followingIndex >= 0)
                {
                    nextIndex = followingIndex;
                }
            }
        }

        this.lastProbedHost = missingHosts[nextIndex];
        return this.lastProbedHost;
    }

    bool HasMissingHost()
    {
        lock (this.syncRoot)
        {
            return this.HasMissingHostLocked();
        }
    }

    bool HasMissingHostLocked()
    {
        HashSet<string> occupiedHosts = new(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(this.primaryHost))
        {
            occupiedHosts.Add(this.primaryHost!);
        }

        foreach (ManagedConnection connection in this.connections.Values)
        {
            occupiedHosts.Add(connection.Host);
        }

        return this.desiredHosts.Any(host => !occupiedHosts.Contains(host));
    }

    async Task<ProbeResult> ProbeOnceAsync(string targetHost, CancellationToken cancellation)
    {
        this.logger.FanOutProbeStarted(this.taskHubName, targetHost);

        IGrpcWorkerConnection workerConnection;
        try
        {
            workerConnection = this.connectionFactory(targetHost);
            if (workerConnection is null)
            {
                throw new InvalidOperationException("The worker connection factory returned null.");
            }
        }
        catch (Exception ex) when (!IsFatal(ex))
        {
            this.logger.FanOutConnectionFailed(ex);
            return ProbeResult.Failed;
        }

        long connectionId = Interlocked.Increment(ref this.nextConnectionId);
        CancellationTokenSource connectionCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        ManagedConnection managedConnection =
            new(connectionId, targetHost, workerConnection, connectionCancellation);
        TaskCompletionSource<P.HealthPing> firstHealthPing =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        int accepted = 0;

        void OnHealthPing(P.HealthPing healthPing)
        {
            if (firstHealthPing.TrySetResult(healthPing))
            {
                return;
            }

            if (Volatile.Read(ref accepted) == 1)
            {
                this.ReportAdditionalHealthPing(managedConnection, healthPing);
            }
        }

        lock (this.syncRoot)
        {
            this.activeProbe = managedConnection;
            this.allConnections.Add(managedConnection);
        }

        try
        {
            managedConnection.RunTask = workerConnection.RunAsync(
                OnHealthPing,
                connectionCancellation.Token,
                this.workerCancellation);
        }
        catch (Exception ex) when (!IsFatal(ex))
        {
            managedConnection.RunTask = Task.FromException(ex);
        }

        Task timeoutTask = this.probeTimeout > TimeSpan.Zero
            ? Task.Delay(this.probeTimeout, cancellation)
            : Task.Delay(Timeout.Infinite, cancellation);
        Task completedTask = await Task.WhenAny(
            firstHealthPing.Task,
            managedConnection.RunTask,
            timeoutTask).ConfigureAwait(false);

        ProbeResult result;
        if (completedTask == firstHealthPing.Task)
        {
            P.HealthPing healthPing = await firstHealthPing.Task.ConfigureAwait(false);
            result = this.TryAcceptProbe(managedConnection, healthPing)
                ? ProbeResult.Accepted
                : ProbeResult.Rejected;
            if (result == ProbeResult.Accepted)
            {
                Volatile.Write(ref accepted, 1);
                this.logger.FanOutConnectionAccepted(managedConnection.Host);
                _ = this.ObserveConnectionAsync(managedConnection);
                this.SignalReconcile();
                return result;
            }

            this.logger.FanOutConnectionRejected(targetHost, healthPing.ConnectedHost);
        }
        else if (completedTask == timeoutTask && !cancellation.IsCancellationRequested)
        {
            this.logger.FanOutProbeTimedOut(targetHost, this.probeTimeout);
            result = ProbeResult.TimedOut;
        }
        else
        {
            result = ProbeResult.Failed;
        }

        lock (this.syncRoot)
        {
            if (ReferenceEquals(this.activeProbe, managedConnection))
            {
                this.activeProbe = null;
            }
        }

        managedConnection.Cancel();
        _ = this.ObserveConnectionAsync(managedConnection);
        this.SignalReconcile();
        return result;
    }

    bool TryAcceptProbe(ManagedConnection connection, P.HealthPing healthPing)
    {
        if (string.IsNullOrWhiteSpace(healthPing.ConnectedHost))
        {
            return false;
        }

        lock (this.syncRoot)
        {
            this.UpdateDesiredHostsLocked(healthPing);
            connection.Host = healthPing.ConnectedHost;

            if (!string.Equals(connection.TargetHost, connection.Host, StringComparison.OrdinalIgnoreCase)
                || !this.desiredHosts.Contains(connection.TargetHost)
                || this.IsHostConnectedLocked(connection.Host))
            {
                return false;
            }

            this.connections.Add(connection.Id, connection);
            if (ReferenceEquals(this.activeProbe, connection))
            {
                this.activeProbe = null;
            }

            return true;
        }
    }

    void ReportAdditionalHealthPing(ManagedConnection connection, P.HealthPing healthPing)
    {
        if (Volatile.Read(ref this.disposed) == 1 || string.IsNullOrWhiteSpace(healthPing.ConnectedHost))
        {
            return;
        }

        bool affinityLost;
        lock (this.syncRoot)
        {
            if (!this.connections.ContainsKey(connection.Id))
            {
                return;
            }

            connection.Host = healthPing.ConnectedHost;
            this.UpdateDesiredHostsLocked(healthPing);
            affinityLost = !string.Equals(
                connection.TargetHost,
                connection.Host,
                StringComparison.OrdinalIgnoreCase);
            if (affinityLost)
            {
                this.connections.Remove(connection.Id);
            }

            this.logger.FanOutTopologyObserved(
                healthPing.ConnectedHost,
                this.taskHubName,
                this.desiredHosts.Count);
        }

        if (affinityLost)
        {
            this.logger.FanOutAffinityLost(connection.TargetHost, connection.Host);
            connection.Cancel();
        }

        this.SignalReconcile();
    }

    bool IsHostConnectedLocked(string host)
    {
        if (string.Equals(this.primaryHost, host, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return this.connections.Values.Any(
            connection => string.Equals(connection.Host, host, StringComparison.OrdinalIgnoreCase));
    }

    void UpdateDesiredHostsLocked(P.HealthPing healthPing)
    {
        this.desiredHosts.Clear();
        foreach (P.TaskHubHosts taskHubHosts in healthPing.TaskHubHosts)
        {
            if (!string.Equals(taskHubHosts.TaskHubName, this.taskHubName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (string host in taskHubHosts.Hosts)
            {
                if (!string.IsNullOrWhiteSpace(host))
                {
                    this.desiredHosts.Add(host);
                }
            }
        }
    }

    async Task ObserveConnectionAsync(ManagedConnection connection)
    {
        await this.ObserveTaskAsync(
            connection.RunTask,
            connection.Cancellation).ConfigureAwait(false);

        lock (this.syncRoot)
        {
            this.connections.Remove(connection.Id);
            if (ReferenceEquals(this.activeProbe, connection))
            {
                this.activeProbe = null;
            }
        }

        this.SignalReconcile();

        CancellationToken shutdownCancellation = this.shutdownSource?.Token ?? CancellationToken.None;
        await this.DisposeConnectionAsync(
            connection,
            this.deferredDisposeDelay,
            shutdownCancellation).ConfigureAwait(false);

        lock (this.syncRoot)
        {
            this.allConnections.Remove(connection);
        }
    }

    async Task ObserveTaskAsync(Task task, CancellationToken expectedCancellation)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (expectedCancellation.IsCancellationRequested)
        {
        }
        catch (Exception ex) when (!IsFatal(ex))
        {
            this.logger.FanOutConnectionFailed(ex);
        }
    }

    async Task DisposeConnectionAsync(
        ManagedConnection connection,
        TimeSpan delay,
        CancellationToken cancellation)
    {
        Task disposalTask = connection.GetOrCreateDisposalTask(
            async () =>
            {
                await DelayAsync(delay, cancellation).ConfigureAwait(false);
                try
                {
                    await connection.Connection.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    connection.CancellationSource.Dispose();
                }
            });

        try
        {
            await disposalTask.ConfigureAwait(false);
        }
        catch (Exception ex) when (!IsFatal(ex))
        {
            this.logger.FanOutConnectionFailed(ex);
        }
    }

    void SignalReconcile()
    {
        if (Volatile.Read(ref this.disposed) == 1)
        {
            return;
        }

        if (Interlocked.Exchange(ref this.reconcilePending, 1) == 0)
        {
            try
            {
                this.reconcileSignal.Release();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    sealed class ManagedConnection
    {
        Task? disposalTask;
        bool disposalStarted;

        public ManagedConnection(
            long id,
            string targetHost,
            IGrpcWorkerConnection connection,
            CancellationTokenSource cancellationSource)
        {
            this.Id = id;
            this.TargetHost = targetHost;
            this.Connection = connection;
            this.CancellationSource = cancellationSource;
            this.Cancellation = cancellationSource.Token;
        }

        public long Id { get; }

        public string TargetHost { get; }

        public IGrpcWorkerConnection Connection { get; }

        public CancellationTokenSource CancellationSource { get; }

        public CancellationToken Cancellation { get; }

        public string Host { get; set; } = string.Empty;

        public Task RunTask { get; set; } = Task.CompletedTask;

        public Task GetOrCreateDisposalTask(Func<Task> disposeAsync)
        {
            lock (this)
            {
                if (!this.disposalStarted)
                {
                    this.disposalStarted = true;
                    this.disposalTask = disposeAsync();
                }

                return this.disposalTask!;
            }
        }

        public void Cancel()
        {
            lock (this)
            {
                if (!this.disposalStarted)
                {
                    this.CancellationSource.Cancel();
                }
            }
        }
    }
}