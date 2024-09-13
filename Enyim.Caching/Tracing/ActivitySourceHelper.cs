#if NET6_0
using System;
using System.Collections.Generic;
using System.Linq;
using Enyim.Caching.Memcached;
using System.Globalization;
using System.Diagnostics;

namespace Enyim.Caching.Tracing
{

    /// <summary>
    /// Static Class to Manage Activities actions for memcacheclient
    /// </summary>
    internal static class ActivitySourceHelper
    {
        public const string ThreadIdTagName = "thread.id";
        public const string StatusCodeTagName = "otel.status_code";
        public const string StatusDescription = "otel.status_description";

        public static Activity? StartActivity(string name, IEnumerable<KeyValuePair<string, object?>>? activityTags = null)
        {
            var activity = ActivitySource.StartActivity(name, ActivityKind.Client, default(ActivityContext), activityTags);
            if (activity is { IsAllDataRequested: true })
            {
                activity.SetTag(ActivitySourceHelper.ThreadIdTagName, Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture));
            }
            return activity;
        }

        public static void AddTagsForKeys(this Activity? activity, IMemcachedNode node, IEnumerable<string> keys)
        {
            const int maxTagLimit = 10; // Set maximum number of keys per tag to avoid memory overflow ( Since multiget can have any number of keys )

            var keysToTag = keys.Take(maxTagLimit).ToList();

            var tag = $"net.peer.query.key:{node.EndPoint}";
            var tagValue = string.Join(",", keysToTag);

            activity?.SetTag(tag, tagValue);
        }

        public static void SetSuccess(this Activity? activity) {
            activity?.SetTag(StatusCodeTagName, "OK");
        } 

        public static void SetException(this Activity activity, Exception exception)
        {
            activity.SetTag(StatusCodeTagName, "ERROR");
            activity.SetTag(StatusDescription, exception?.Message);
            activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception?.GetType().FullName },
                { "exception.message", exception?.Message },
                { "exception.stacktrace", exception?.ToString() },
            }));
        }

        private static ActivitySource ActivitySource { get; } = CreateActivitySource();

        private static ActivitySource CreateActivitySource()
        {
            return new("MemcacheConnector");
        }
    }
}
# endif