using Newtonsoft.Json;

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
        public string ETag { get; internal set; }

        internal Lock() { }
    }
}
