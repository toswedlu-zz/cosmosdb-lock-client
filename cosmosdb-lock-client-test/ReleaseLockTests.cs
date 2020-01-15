using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Net;
using System.Threading;

namespace cosmosdb_lock_client_test
{
    [TestClass]
    public class ReleaseLockTests
    {
        [TestMethod]
        public void WithAcquiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            Lock @lock = client.Acquire(options);
            client.Release(@lock);
            try
            {
                client.Acquire(options);
            }
            catch (LockUnavailableException)
            {
                Assert.Fail("Lock unavailable after release.");
            }
        }

        [TestMethod]
        public void WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            Lock @lock = client.Acquire(options);
            Thread.Sleep(options.LeaseDuration * 1000 + 100);
            try
            {
                client.Release(@lock);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void IsNotAcquired()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            Lock @lock = lockClient.Acquire(options);
            lockClient.Release(@lock);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public void LockHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            Assert.ThrowsException<ArgumentNullException>(() => lockClient.Release(null));
        }

        [TestMethod]
        public void CosmosExceptionThrown()
        {
            // Note: for the cosmos exception to be thrown, the status code needs to be anything but PreconditionFailed.
            CosmosException innerEx = new CosmosException(string.Empty, HttpStatusCode.OK, 0, string.Empty, 0);
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRelease = new AggregateException(innerEx);
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<AggregateException>(() => client.Release(@lock));
        }

        [TestMethod]
        public void OtherExceptionThrown()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRelease = new Exception();
            LockClient client = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<Exception>(() => client.Release(@lock));
        }
    }
}
