// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using Grpc.Core;
using Microsoft.DurableTask.Entities;
using Microsoft.DurableTask.Tests.Logging;
using Microsoft.DurableTask.Worker.Grpc.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc.Tests;

public class OperationCompletedTests
{
    const string Category = "Microsoft.DurableTask.Worker.Grpc";

    static readonly Type ProcessorType = typeof(GrpcDurableTaskWorker)
        .GetNestedType("Processor", BindingFlags.NonPublic)!;
    static readonly MethodInfo CompleteOrchestratorTaskMethod = ProcessorType
        .GetMethod("CompleteOrchestratorTaskWithChunkingAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
    static readonly MethodInfo OnRunActivityMethod = ProcessorType
        .GetMethod("OnRunActivityAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
    static readonly MethodInfo OnRunEntityBatchMethod = ProcessorType
        .GetMethod("OnRunEntityBatchAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public async Task CompleteOrchestratorTask_AfterFinalAck_NotifiesGroupedActionsOnce()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        TaskCompletionSource<P.CompleteTaskResponse> finalAck = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int completionCalls = 0;
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
        };
        GrpcDurableTaskWorkerOptions grpcOptions = new();
        grpcOptions.Capabilities.Add(P.WorkerCapability.LargePayloads);
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteOrchestratorTaskAsync(
                It.IsAny<P.OrchestratorResponse>(),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                completionCalls++;
                return completionCalls == 3
                    ? CreateUnaryCall(finalAck.Task)
                    : CreateUnaryCall(Task.FromResult(new P.CompleteTaskResponse()));
            });

        P.OrchestratorResponse response = new()
        {
            InstanceId = "instance",
            CompletionToken = "token",
            Actions =
            {
                new P.OrchestratorAction
                {
                    ScheduleTask = new P.ScheduleTaskAction { Name = "A", Input = new string('a', 200) },
                },
                new P.OrchestratorAction
                {
                    ScheduleTask = new P.ScheduleTaskAction { Name = "B", Input = new string('b', 200) },
                },
                new P.OrchestratorAction
                {
                    CreateTimer = new P.CreateTimerAction(),
                },
            },
        };

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            grpcOptions,
            workerOptions,
            new TestFactory(),
            services,
            clientMock.Object);

        // Act
        Task completion = InvokeAsync(CompleteOrchestratorTaskMethod, processor, response, 150, CancellationToken.None);
        await WaitUntilAsync(() => completionCalls == 3);

        // Assert
        notifications.Should().BeEmpty();
        finalAck.SetResult(new P.CompleteTaskResponse());
        await completion;
        completionCalls.Should().Be(3);
        notifications.Should().BeEquivalentTo(
            [
                new
                {
                    Kind = DurableTaskWorkerOperationKind.OrchestrationAction,
                    Name = "ScheduleTask",
                    Count = 2,
                    TaskName = (string?)null,
                },
                new
                {
                    Kind = DurableTaskWorkerOperationKind.OrchestrationAction,
                    Name = "CreateTimer",
                    Count = 1,
                    TaskName = (string?)null,
                },
            ]);
    }

    [Fact]
    public async Task CompleteOrchestratorTask_OversizedAction_NotifiesOnlyAcceptedFailureAction()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        P.OrchestratorResponse? acceptedResponse = null;
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
        };
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteOrchestratorTaskAsync(
                It.IsAny<P.OrchestratorResponse>(),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Callback((P.OrchestratorResponse response, Metadata _, DateTime? _, CancellationToken _) =>
                acceptedResponse = response)
            .Returns(CreateUnaryCall(Task.FromResult(new P.CompleteTaskResponse())));

        P.OrchestratorResponse response = new()
        {
            InstanceId = "instance",
            CompletionToken = "token",
            Actions =
            {
                new P.OrchestratorAction
                {
                    ScheduleTask = new P.ScheduleTaskAction { Name = "A", Input = new string('a', 200) },
                },
            },
        };

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(),
            services,
            clientMock.Object);

        // Act
        await InvokeAsync(CompleteOrchestratorTaskMethod, processor, response, 100, CancellationToken.None);

        // Assert
        acceptedResponse!.Actions.Should().ContainSingle(action => action.CompleteOrchestration != null);
        notifications.Should().ContainSingle().Which.Should().BeEquivalentTo(new
        {
            Kind = DurableTaskWorkerOperationKind.OrchestrationAction,
            Name = "CompleteOrchestration",
            Count = 1,
            TaskName = (string?)null,
        });
    }

    [Fact]
    public async Task OnRunActivity_AfterAck_NotifiesFailedApplicationActivityAsCompleted()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        TaskCompletionSource<P.CompleteTaskResponse> ack = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
        };
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteActivityTaskAsync(
                It.Is<P.ActivityResponse>(response => response.FailureDetails != null),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(CreateUnaryCall(ack.Task));

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(activity: new ThrowingActivity()),
            services,
            clientMock.Object);
        P.ActivityRequest request = new()
        {
            Name = "FailingActivity",
            TaskId = 1,
            OrchestrationInstance = new P.OrchestrationInstance
            {
                InstanceId = "instance",
                ExecutionId = "execution",
            },
        };

        // Act
        Task completion = InvokeAsync(
            OnRunActivityMethod,
            processor,
            request,
            "token",
            CancellationToken.None);
        await WaitUntilAsync(() => clientMock.Invocations.Count > 0);

        // Assert
        notifications.Should().BeEmpty();
        ack.SetResult(new P.CompleteTaskResponse());
        await completion;
        notifications.Should().ContainSingle().Which.Should().BeEquivalentTo(new
        {
            Kind = DurableTaskWorkerOperationKind.Activity,
            Name = "FailingActivity",
            Count = 1,
            TaskName = (string?)null,
        });
    }

    [Fact]
    public async Task OnRunActivity_VersionMismatchAbandoned_DoesNotNotify()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
            Versioning = new DurableTaskWorkerOptions.VersioningOptions
            {
                Version = "1",
                MatchStrategy = DurableTaskWorkerOptions.VersionMatchStrategy.Strict,
                FailureStrategy = DurableTaskWorkerOptions.VersionFailureStrategy.Reject,
            },
        };
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.AbandonTaskActivityWorkItemAsync(
                It.IsAny<P.AbandonActivityTaskRequest>(),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(CreateUnaryCall(Task.FromResult(new P.AbandonActivityTaskResponse())));

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(),
            services,
            clientMock.Object);
        P.ActivityRequest request = new()
        {
            Name = "Activity",
            Version = "2",
            TaskId = 1,
            OrchestrationInstance = new P.OrchestrationInstance
            {
                InstanceId = "instance",
                ExecutionId = "execution",
            },
        };

        // Act
        await InvokeAsync(OnRunActivityMethod, processor, request, "token", CancellationToken.None);

        // Assert
        notifications.Should().BeEmpty();
        clientMock.VerifyAll();
    }

    [Fact]
    public async Task OnRunEntityBatch_AfterAck_NotifiesGroupedCompletedOperations()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        TaskCompletionSource<P.CompleteTaskResponse> ack = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
        };
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteEntityTaskAsync(
                It.Is<P.EntityBatchResult>(response => response.Results.Count == 3),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(CreateUnaryCall(ack.Task));
        EntityBatchRequest request = CreateEntityBatchRequest("add", "add", "get");

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(),
            services,
            clientMock.Object);

        // Act
        Task completion = InvokeAsync(
            OnRunEntityBatchMethod,
            processor,
            request,
            CancellationToken.None,
            "token",
            null);
        await WaitUntilAsync(() => clientMock.Invocations.Count > 0);

        // Assert
        notifications.Should().BeEmpty();
        ack.SetResult(new P.CompleteTaskResponse());
        await completion;
        notifications.Should().BeEquivalentTo(
            [
                new
                {
                    Kind = DurableTaskWorkerOperationKind.EntityOperation,
                    Name = "add",
                    Count = 2,
                    TaskName = "counter",
                },
                new
                {
                    Kind = DurableTaskWorkerOperationKind.EntityOperation,
                    Name = "get",
                    Count = 1,
                    TaskName = "counter",
                },
            ]);
    }

    [Fact]
    public async Task OnRunEntityBatch_FrameworkFailureWithNoResults_DoesNotNotify()
    {
        // Arrange
        ConcurrentQueue<DurableTaskWorkerOperation> notifications = new();
        DurableTaskWorkerOptions workerOptions = new()
        {
            OperationCompleted = notifications.Enqueue,
        };
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteEntityTaskAsync(
                It.Is<P.EntityBatchResult>(response =>
                    response.FailureDetails != null && response.Results.Count == 0),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(CreateUnaryCall(Task.FromResult(new P.CompleteTaskResponse())));

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(entityFailure: new InvalidOperationException("framework failure")),
            services,
            clientMock.Object);

        // Act
        await InvokeAsync(
            OnRunEntityBatchMethod,
            processor,
            CreateEntityBatchRequest("add"),
            CancellationToken.None,
            "token",
            null);

        // Assert
        notifications.Should().BeEmpty();
        clientMock.VerifyAll();
    }

    [Fact]
    public async Task CompleteOrchestratorTask_CallbackThrows_CompletionStillSucceedsAndLogs()
    {
        // Arrange
        DurableTaskWorkerOptions workerOptions = new()
        {
            Logging = { UseLegacyCategories = false },
            OperationCompleted = _ => throw new InvalidOperationException("callback failed"),
        };
        TestLogProvider logProvider = new(new NullOutput());
        Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> clientMock = CreateClientMock();
        clientMock
            .Setup(client => client.CompleteOrchestratorTaskAsync(
                It.IsAny<P.OrchestratorResponse>(),
                It.IsAny<Metadata>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .Returns(CreateUnaryCall(Task.FromResult(new P.CompleteTaskResponse())));

        P.OrchestratorResponse response = new()
        {
            InstanceId = "instance",
            CompletionToken = "token",
            Actions =
            {
                new P.OrchestratorAction { CreateTimer = new P.CreateTimerAction() },
            },
        };

        using ServiceProvider services = new ServiceCollection().BuildServiceProvider();
        object processor = CreateProcessor(
            new GrpcDurableTaskWorkerOptions(),
            workerOptions,
            new TestFactory(),
            services,
            clientMock.Object,
            new SimpleLoggerFactory(logProvider));

        // Act
        Func<Task> act = () => InvokeAsync(
            CompleteOrchestratorTaskMethod,
            processor,
            response,
            int.MaxValue,
            CancellationToken.None);

        // Assert
        await act.Should().NotThrowAsync();
        logProvider.TryGetLogs(Category, out IReadOnlyCollection<LogEntry>? logs).Should().BeTrue();
        logs.Should().Contain(log =>
            log.Message.Contains("Worker operation completion callback failed")
            && log.Message.Contains("CreateTimer"));
    }

    static EntityBatchRequest CreateEntityBatchRequest(params string[] operationNames)
    {
        return new EntityBatchRequest
        {
            InstanceId = "@counter@key",
            Operations = operationNames
                .Select(name => new OperationRequest { Operation = name })
                .ToList(),
        };
    }

    static Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient> CreateClientMock()
    {
        return new Mock<P.TaskHubSidecarService.TaskHubSidecarServiceClient>(
            MockBehavior.Strict,
            new object[] { Mock.Of<CallInvoker>() });
    }

    static object CreateProcessor(
        GrpcDurableTaskWorkerOptions grpcOptions,
        DurableTaskWorkerOptions workerOptions,
        IDurableTaskFactory factory,
        IServiceProvider services,
        P.TaskHubSidecarService.TaskHubSidecarServiceClient client,
        ILoggerFactory? loggerFactory = null)
    {
        GrpcDurableTaskWorker worker = new(
            name: "Test",
            factory: factory,
            grpcOptions: new OptionsMonitorStub<GrpcDurableTaskWorkerOptions>(grpcOptions),
            workerOptions: new OptionsMonitorStub<DurableTaskWorkerOptions>(workerOptions),
            services: services,
            loggerFactory: loggerFactory ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            orchestrationFilter: null,
            exceptionPropertiesProvider: null,
            workItemFiltersMonitor: null);

        return Activator.CreateInstance(
            ProcessorType,
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            args: [worker, client, null, null],
            culture: null)!;
    }

    static Task InvokeAsync(MethodInfo method, object instance, params object?[] arguments)
    {
        return (Task)method.Invoke(instance, arguments)!;
    }

    static AsyncUnaryCall<T> CreateUnaryCall<T>(Task<T> responseTask)
    {
        return new AsyncUnaryCall<T>(
            responseTask,
            Task.FromResult(new Metadata()),
            () => Status.DefaultSuccess,
            () => new Metadata(),
            () => { });
    }

    static async Task WaitUntilAsync(Func<bool> condition)
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    sealed class TestFactory : IDurableTaskFactory2
    {
        readonly ITaskActivity? activity;
        readonly Exception? entityFailure;

        public TestFactory(ITaskActivity? activity = null, Exception? entityFailure = null)
        {
            this.activity = activity;
            this.entityFailure = entityFailure;
        }

        public bool TryCreateActivity(
            TaskName name,
            IServiceProvider serviceProvider,
            [NotNullWhen(true)] out ITaskActivity? activity)
        {
            activity = this.activity;
            return activity is not null;
        }

        public bool TryCreateOrchestrator(
            TaskName name,
            IServiceProvider serviceProvider,
            [NotNullWhen(true)] out ITaskOrchestrator? orchestrator)
        {
            orchestrator = null;
            return false;
        }

        public bool TryCreateEntity(
            TaskName name,
            IServiceProvider serviceProvider,
            [NotNullWhen(true)] out ITaskEntity? entity)
        {
            if (this.entityFailure is not null)
            {
                throw this.entityFailure;
            }

            entity = null;
            return false;
        }
    }

    sealed class ThrowingActivity : ITaskActivity
    {
        public Type InputType => typeof(object);

        public Type OutputType => typeof(object);

        public Task<object?> RunAsync(TaskActivityContext context, object? input)
        {
            throw new InvalidOperationException("application failure");
        }
    }

    sealed class NullOutput : ITestOutputHelper
    {
        public void WriteLine(string message)
        {
        }

        public void WriteLine(string format, params object[] args)
        {
        }
    }
}
