// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.DurableTask.Worker.Grpc.Internal;
using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc.Tests;

public class GrpcDurableTaskWorkerOptionsInternalTests
{
    [Fact]
    public void InternalOptions_HasSafeDefaults()
    {
        // Arrange
        GrpcDurableTaskWorkerOptions options = new();

        // Act
        GrpcDurableTaskWorkerOptions.InternalOptions internalOptions = options.Internal;

        // Assert
        internalOptions.HelloDeadline.Should().Be(TimeSpan.FromSeconds(30));
        internalOptions.ChannelRecreateFailureThreshold.Should().Be(5);
        internalOptions.ReconnectBackoffBase.Should().Be(TimeSpan.FromSeconds(1));
        internalOptions.ReconnectBackoffCap.Should().Be(TimeSpan.FromSeconds(30));
        internalOptions.TransientRetryBackoffBase.Should().Be(TimeSpan.FromMilliseconds(200));
        internalOptions.TransientRetryBackoffCap.Should().Be(TimeSpan.FromSeconds(15));
        internalOptions.TransientRetryMaxAttempts.Should().Be(10);
        internalOptions.SilentDisconnectTimeout.Should().Be(TimeSpan.FromSeconds(120));
        internalOptions.ChannelRecreator.Should().BeNull();
        internalOptions.TaskHubName.Should().BeNull();
        internalOptions.AdditionalChannelFactory.Should().BeNull();
        internalOptions.FanOutConnectionTimeout.Should().Be(TimeSpan.FromSeconds(120));
        internalOptions.FanOutDiscoveryTimeout.Should().Be(TimeSpan.FromSeconds(10));
        internalOptions.FanOutRetryDelay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void SetChannelRecreator_NullCallback_Throws()
    {
        // Arrange
        GrpcDurableTaskWorkerOptions options = new();

        // Act
        Action act = () => options.SetChannelRecreator(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void SetChannelRecreator_StoresCallbackOnInternalOptions()
    {
        // Arrange
        GrpcDurableTaskWorkerOptions options = new();
        bool invoked = false;
        Func<GrpcChannel, CancellationToken, Task<GrpcChannel>> recreator = (channel, ct) =>
        {
            invoked = true;
            return Task.FromResult(channel);
        };

        // Act
        options.SetChannelRecreator(recreator);

        // Assert
        options.Internal.ChannelRecreator.Should().BeSameAs(recreator);

        // Sanity-check that invoking the stored delegate calls the original.
        options.Internal.ChannelRecreator!.Invoke(null!, CancellationToken.None);
        invoked.Should().BeTrue();
    }

    [Fact]
    public void ConfigureConnectionFanOut_StoresTaskHubAndChannelFactory()
    {
        // Arrange
        GrpcDurableTaskWorkerOptions options = new();
        using GrpcChannel channel = GrpcChannel.ForAddress("http://localhost");
        Func<string, GrpcChannel> channelFactory = _ => channel;

        // Act
        options.ConfigureConnectionFanOut("test-hub", channelFactory);

        // Assert
        options.Internal.TaskHubName.Should().Be("test-hub");
        options.Internal.AdditionalChannelFactory.Should().BeSameAs(channelFactory);
        options.Capabilities.Should().Contain(P.WorkerCapability.FanOut);
        ((int)P.WorkerCapability.FanOut).Should().Be(4);
    }
}