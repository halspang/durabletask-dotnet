// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging.Abstractions;
using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc.Tests;

public class GrpcWorkerConnectionManagerTests
{
    const string TaskHubName = "test-hub";
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task MissingHost_DuplicateProbeIsRetriedUntilHostIsConnected()
    {
        // Arrange
        FakeWorkerConnection duplicateConnection = new(CreateHealthPing("host-a", "host-a", "host-b"));
        FakeWorkerConnection desiredConnection = new(CreateHealthPing("host-b", "host-a", "host-b"));
        Queue<FakeWorkerConnection> connections = new(new[] { duplicateConnection, desiredConnection });
        await using GrpcWorkerConnectionManager manager = CreateManager(() => connections.Dequeue());
        manager.Start(CancellationToken.None);

        // Act
        manager.ReportPrimaryHealthPing(CreateHealthPing("host-a", "host-a", "host-b"));
        await desiredConnection.Started.Task.WaitAsync(TestTimeout);
        await WaitUntilAsync(
            () => manager.ConnectedHosts.OrderBy(host => host).SequenceEqual(new[] { "host-a", "host-b" }),
            TestTimeout);

        // Assert
        duplicateConnection.ConnectionStopped.Task.IsCompleted.Should().BeTrue();
        await duplicateConnection.Disposed.Task.WaitAsync(TestTimeout);
        duplicateConnection.WorkerCancellation.IsCancellationRequested.Should().BeFalse();
        manager.ConnectedHosts.Should().BeEquivalentTo("host-a", "host-b");
    }

    [Fact]
    public async Task HostNoLongerAdvertised_StopsAndDisposesOnlyThatConnection()
    {
        // Arrange
        FakeWorkerConnection additionalConnection = new(CreateHealthPing("host-b", "host-a", "host-b"));
        await using GrpcWorkerConnectionManager manager = CreateManager(() => additionalConnection);
        manager.Start(CancellationToken.None);
        manager.ReportPrimaryHealthPing(CreateHealthPing("host-a", "host-a", "host-b"));
        await additionalConnection.Started.Task.WaitAsync(TestTimeout);
        await WaitUntilAsync(() => manager.ConnectedHosts.Contains("host-b"), TestTimeout);

        // Act
        manager.ReportPrimaryHealthPing(CreateHealthPing("host-a", "host-a"));
        await additionalConnection.ConnectionStopped.Task.WaitAsync(TestTimeout);
        await additionalConnection.Disposed.Task.WaitAsync(TestTimeout);

        // Assert
        additionalConnection.ConnectionCancellation.IsCancellationRequested.Should().BeTrue();
        additionalConnection.WorkerCancellation.IsCancellationRequested.Should().BeFalse();
        manager.ConnectedHosts.Should().BeEquivalentTo("host-a");
    }

    [Fact]
    public async Task EmptyLegacyHealthPing_DoesNotOpenAdditionalConnection()
    {
        // Arrange
        int connectionFactoryCalls = 0;
        await using GrpcWorkerConnectionManager manager = CreateManager(
            () =>
            {
                Interlocked.Increment(ref connectionFactoryCalls);
                return new FakeWorkerConnection(null);
            });
        manager.Start(CancellationToken.None);

        // Act
        manager.ReportPrimaryHealthPing(new P.HealthPing());
        await Task.Delay(TimeSpan.FromMilliseconds(100));

        // Assert
        connectionFactoryCalls.Should().Be(0);
        manager.ConnectedHosts.Should().BeEmpty();
    }

    [Fact]
    public async Task OtherTaskHubAdvertisesHost_DoesNotOpenAdditionalConnection()
    {
        // Arrange
        int connectionFactoryCalls = 0;
        await using GrpcWorkerConnectionManager manager = CreateManager(
            () =>
            {
                Interlocked.Increment(ref connectionFactoryCalls);
                return new FakeWorkerConnection(null);
            });
        manager.Start(CancellationToken.None);
        P.HealthPing healthPing = new()
        {
            ConnectedHost = "host-a",
        };
        P.TaskHubHosts otherTaskHubHosts = new()
        {
            TaskHubName = "other-hub",
        };
        otherTaskHubHosts.Hosts.Add("host-b");
        healthPing.TaskHubHosts.Add(otherTaskHubHosts);

        // Act
        manager.ReportPrimaryHealthPing(healthPing);
        await Task.Delay(TimeSpan.FromMilliseconds(100));

        // Assert
        connectionFactoryCalls.Should().Be(0);
        manager.ConnectedHosts.Should().BeEquivalentTo("host-a");
    }

    [Fact]
    public async Task ManagerDisposal_StopsAndDisposesAcceptedConnections()
    {
        // Arrange
        FakeWorkerConnection additionalConnection = new(CreateHealthPing("host-b", "host-a", "host-b"));
        GrpcWorkerConnectionManager manager = CreateManager(() => additionalConnection);
        manager.Start(CancellationToken.None);
        manager.ReportPrimaryHealthPing(CreateHealthPing("host-a", "host-a", "host-b"));
        await additionalConnection.Started.Task.WaitAsync(TestTimeout);
        await WaitUntilAsync(() => manager.ConnectedHosts.Contains("host-b"), TestTimeout);

        // Act
        await manager.DisposeAsync();

        // Assert
        additionalConnection.ConnectionCancellation.IsCancellationRequested.Should().BeTrue();
        additionalConnection.WorkerCancellation.IsCancellationRequested.Should().BeFalse();
        additionalConnection.Disposed.Task.IsCompleted.Should().BeTrue();
    }

    static GrpcWorkerConnectionManager CreateManager(Func<IGrpcWorkerConnection> connectionFactory)
        => new(
            TaskHubName,
            connectionFactory,
            NullLogger.Instance,
            probeTimeout: TestTimeout,
            retryDelay: TimeSpan.Zero,
            deferredDisposeDelay: TimeSpan.Zero);

    static P.HealthPing CreateHealthPing(string connectedHost, params string[] availableHosts)
    {
        P.TaskHubHosts taskHubHosts = new()
        {
            TaskHubName = TaskHubName,
        };
        taskHubHosts.Hosts.Add(availableHosts);

        P.HealthPing healthPing = new()
        {
            ConnectedHost = connectedHost,
        };
        healthPing.TaskHubHosts.Add(taskHubHosts);
        return healthPing;
    }

    static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow.Add(timeout);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("The condition was not met before the timeout.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }
    }

    sealed class FakeWorkerConnection : IGrpcWorkerConnection
    {
        readonly P.HealthPing? healthPing;

        public FakeWorkerConnection(P.HealthPing? healthPing)
        {
            this.healthPing = healthPing;
        }

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ConnectionStopped { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Disposed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationToken ConnectionCancellation { get; private set; }

        public CancellationToken WorkerCancellation { get; private set; }

        public async Task RunAsync(
            Action<P.HealthPing> onHealthPing,
            CancellationToken connectionCancellation,
            CancellationToken workerCancellation)
        {
            this.ConnectionCancellation = connectionCancellation;
            this.WorkerCancellation = workerCancellation;
            this.Started.TrySetResult();
            if (this.healthPing is not null)
            {
                onHealthPing(this.healthPing);
            }

            try
            {
                await Task.Delay(Timeout.Infinite, connectionCancellation);
            }
            catch (OperationCanceledException) when (connectionCancellation.IsCancellationRequested)
            {
            }
            finally
            {
                this.ConnectionStopped.TrySetResult();
            }
        }

        public ValueTask DisposeAsync()
        {
            this.Disposed.TrySetResult();
            return default;
        }
    }
}
