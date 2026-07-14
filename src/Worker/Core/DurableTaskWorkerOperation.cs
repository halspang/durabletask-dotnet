// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Microsoft.DurableTask.Worker;

/// <summary>
/// The kind of user operation completed by a Durable Task worker.
/// </summary>
public enum DurableTaskWorkerOperationKind
{
    /// <summary>
    /// An action emitted by an orchestrator.
    /// </summary>
    OrchestrationAction = 0,

    /// <summary>
    /// An activity execution.
    /// </summary>
    Activity = 1,

    /// <summary>
    /// An entity operation.
    /// </summary>
    EntityOperation = 2,
}

/// <summary>
/// Describes one or more completed user operations accepted by the Durable Task backend.
/// </summary>
public readonly struct DurableTaskWorkerOperation
{
    internal DurableTaskWorkerOperation(
        DurableTaskWorkerOperationKind kind,
        string name,
        int count,
        string? taskName = null)
    {
        this.Kind = kind;
        this.Name = name;
        this.Count = count;
        this.TaskName = taskName;
    }

    /// <summary>
    /// Gets the kind of completed operation.
    /// </summary>
    public DurableTaskWorkerOperationKind Kind { get; }

    /// <summary>
    /// Gets the operation name.
    /// </summary>
    /// <remarks>
    /// This is the orchestrator action type, activity name, or entity operation name, depending on
    /// <see cref="Kind"/>.
    /// </remarks>
    public string Name { get; }

    /// <summary>
    /// Gets the number of completed operations represented by this value.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Gets the registered task name when it is distinct from <see cref="Name"/>, or <c>null</c>.
    /// </summary>
    /// <remarks>
    /// Entity operation notifications use this property for the registered entity name.
    /// </remarks>
    public string? TaskName { get; }
}
