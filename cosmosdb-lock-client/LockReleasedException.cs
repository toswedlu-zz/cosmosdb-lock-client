using System;

namespace Microsoft.Azure.Cosmos.Locking
{
    public class LockReleasedException : Exception
    {
        static string _message = "The lock with parition key: \"{0}\" and name: \"{1}\" has been released/expired and no longer exists.";

        public LockReleasedException(Lock @lock, Exception innerEx = null)
            : base(string.Format(_message, @lock.PartitionKey, @lock.Name), innerEx)
        {
        }
    }
}