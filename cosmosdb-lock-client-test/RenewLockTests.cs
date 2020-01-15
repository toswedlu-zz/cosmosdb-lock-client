using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace cosmosdb_lock_client_test
{
    [TestClass]
    public class RenewLockTests
    {
        [TestMethod]
        public void WithAcquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 120
            };
            Lock @lock = lockClient.Acquire(options);
            DateTime origTimeAcquired = @lock.TimeAcquired;
            string origEtag = @lock.ETag;
            lockClient.Renew(@lock);
            Assert.IsTrue(@lock.TimeAcquired > origTimeAcquired);
            Assert.AreNotEqual(@lock.ETag, origEtag);
        }

        [TestMethod]
        public void WithReacquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 2
            };
            Lock @lock = lockClient.Acquire(options);
            Thread.Sleep(options.LeaseDuration * 1000);
            Lock newLock = lockClient.Acquire(options);
            Assert.ThrowsException<LockReleasedException>(() => lockClient.Renew(@lock));
        }

        [TestMethod]
        public void WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDuration = 2
            };
            Lock @lock = lockClient.Acquire(options);
            Thread.Sleep(options.LeaseDuration * 1000);
            Assert.ThrowsException<LockReleasedException>(() => lockClient.Renew(@lock));
        }

        [TestMethod]
        public void LockHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            Assert.ThrowsException<ArgumentNullException>(() => lockClient.Renew(null));
        }

        [TestMethod]
        public void CosmosExceptionThrown()
        {
            // Note: for the cosmos exception to be thrown, the status code needs to be anything but PreconditionFailed or NotFound.
            CosmosException innerEx = new CosmosException(string.Empty, System.Net.HttpStatusCode.OK, 0, string.Empty, 0);
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRenew = new AggregateException(innerEx);
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<AggregateException>(() => client.Renew(@lock));
        }

        [TestMethod]
        public void OtherExceptionThrown()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRenew = new Exception();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions acquireOptions = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            Lock @lock = client.Acquire(acquireOptions);
            Assert.ThrowsException<Exception>(() => client.Renew(@lock));
        }
    }
}
