﻿using System;

namespace Microsoft.Azure.Cosmos
{
    public class LockUnavailableException : Exception
    {
        static string _message = "The lock with partition key: \"{0}\" and name: \"{1}\" is unavailable.";

        public LockUnavailableException(string partitionKey, string name)
            : base(string.Format(_message, partitionKey, name))
        {
        }

        public LockUnavailableException(Lock @lock)
            : base(string.Format(_message, @lock.PartitionKey, @lock.Name))
        {
        }
    }
}