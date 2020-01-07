using Newtonsoft.Json;
using System;

namespace Microsoft.Azure.Cosmos
{
    public class Lock
    {
        [JsonProperty(PropertyName = "partitionKey")]
        public string PartitionKey { get; internal set; }

        [JsonProperty(PropertyName = "id")]
        public string Name { get; internal set; }

        [JsonProperty(PropertyName = "ttl")]
        public int LeaseDuration { get; internal set; }

        [JsonIgnore]
        public DateTime TimeAcquired { get; internal set; }

        [JsonIgnore]
        public string ETag { get; internal set; }

        bool _released = false;
        [JsonIgnore]
        public bool IsAquired
        {
            get { return !_released && (LockUtils.Now - TimeAcquired).TotalSeconds < LeaseDuration; }
            internal set { _released = true; }
        }

        internal Lock() { }
    }
}
