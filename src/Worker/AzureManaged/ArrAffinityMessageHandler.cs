// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Microsoft.DurableTask;

/// <summary>
/// Adds an App Service ARR affinity cookie to every request sent through a dedicated scheduler channel.
/// </summary>
sealed class ArrAffinityMessageHandler : DelegatingHandler
{
    const string CookieHeaderName = "Cookie";
    readonly string cookieHeaderValue;

    /// <summary>
    /// Initializes a new instance of the <see cref="ArrAffinityMessageHandler"/> class.
    /// </summary>
    /// <param name="instanceId">The App Service instance ID to target.</param>
    /// <param name="innerHandler">The handler that sends the request.</param>
    public ArrAffinityMessageHandler(string instanceId, HttpMessageHandler innerHandler)
        : base(innerHandler)
    {
        if (string.IsNullOrWhiteSpace(instanceId))
        {
            throw new ArgumentException("The App Service instance ID must not be empty.", nameof(instanceId));
        }

        if (instanceId.Any(character => !IsCookieValueCharacter(character)))
        {
            throw new ArgumentException(
                "The App Service instance ID contains characters that are not valid in an ARR affinity cookie.",
                nameof(instanceId));
        }

        this.cookieHeaderValue = $"ARRAffinity={instanceId}";
    }

    /// <inheritdoc/>
    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        request.Headers.Remove(CookieHeaderName);
        if (!request.Headers.TryAddWithoutValidation(CookieHeaderName, this.cookieHeaderValue))
        {
            throw new InvalidOperationException("Failed to add the ARR affinity cookie to the scheduler request.");
        }

        return base.SendAsync(request, cancellationToken);
    }

    static bool IsCookieValueCharacter(char character)
        => character <= 0x7F
            && (char.IsLetterOrDigit(character) || character is '-' or '_' or '.' or '~');
}