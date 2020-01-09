namespace Microsoft.Azure.Cosmos
{
    public class AcquireLockOptions
    {
        /**
         */
        public string PartitionKey { get; set; }

        /**
         */
        public string LockName { get; set; }

        /**
         */
        public int LeaseDuration { get; set; } = 60;

        /**
         */
        public int TimeoutMS { get; set; } = 0;

        /**
         */
        public int RetryWaitMS { get; set; } = 1000;
    }
}
