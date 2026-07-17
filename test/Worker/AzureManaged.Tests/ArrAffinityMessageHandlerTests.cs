// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using FluentAssertions;
using Xunit;

namespace Microsoft.DurableTask.Worker.AzureManaged.Tests;

public class ArrAffinityMessageHandlerTests
{
    [Fact]
    public async Task SendAsync_AddsOnlyRequestedArrAffinityCookie()
    {
        // Arrange
        string? cookieHeader = null;
        CallbackHttpMessageHandler innerHandler = new(
            request =>
            {
                cookieHeader = string.Join("; ", request.Headers.GetValues("Cookie"));
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            });
        using HttpMessageInvoker invoker = new(
            new ArrAffinityMessageHandler("instance-a", innerHandler));
        using HttpRequestMessage request = new(HttpMethod.Post, "https://scheduler.test");
        request.Headers.TryAddWithoutValidation("Cookie", "ARRAffinity=stale; OtherCookie=value");

        // Act
        using HttpResponseMessage response =
            await invoker.SendAsync(request, CancellationToken.None);

        // Assert
        response.IsSuccessStatusCode.Should().BeTrue();
        cookieHeader.Should().Be("ARRAffinity=instance-a");
    }

    [Fact]
    public void Constructor_UnsafeInstanceId_Throws()
    {
        // Arrange
        using CallbackHttpMessageHandler innerHandler = new(
            _ => new HttpResponseMessage(System.Net.HttpStatusCode.OK));

        // Act
        Action action = () => new ArrAffinityMessageHandler("instance-a\r\nx-header: value", innerHandler);

        // Assert
        action.Should().Throw<ArgumentException>();
    }

    sealed class CallbackHttpMessageHandler : HttpMessageHandler
    {
        readonly Func<HttpRequestMessage, HttpResponseMessage> callback;

        public CallbackHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> callback)
        {
            this.callback = callback;
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => Task.FromResult(this.callback(request));
    }
}