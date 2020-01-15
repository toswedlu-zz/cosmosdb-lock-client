using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace cosmosdb_lock_client_test
{
    [TestClass]
    public class AcquireLockTests
    {
        [TestMethod]
        public void FailAfterTimeout()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 2000,
                RetryWaitMS = 1000
            };
            lockClient.Acquire(options);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(4, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public void FailWithZeroTimeout()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 0
            };
            lockClient.Acquire(options);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(2, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public void WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            string etag = lockClient.Acquire(options).ETag;
            Thread.Sleep(1100);
            try
            {
                Assert.AreNotEqual(etag, lockClient.Acquire(options).ETag);
            }
            catch (LockUnavailableException)
            {
                Assert.Fail("Lock unavailable after expiration.");
            }
        }

        [TestMethod]
        public void IsAcquired()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 2
            };
            Lock @lock = lockClient.Acquire(options);
            Assert.IsTrue(@lock.IsAquired);
            Thread.Sleep(options.LeaseDuration * 1000);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public void ParitionKeyHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = string.Empty,
                LockName = "test-name"
            };
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.PartitionKey = null;
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            try
            {
                options.PartitionKey = "test-key";
                lockClient.Acquire(options);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void LockNameHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = string.Empty
            };
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.LockName = null;
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            try
            {
                options.LockName = "test-lock";
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void OptionsHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            try
            {
                lockClient.Acquire(options);
            }
            catch
            {
                Assert.Fail();
            }

            Assert.ThrowsException<ArgumentNullException>(() => lockClient.Acquire(null));
        }

        [TestMethod]
        public void ETagAssigned()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            Lock @lock = lockClient.Acquire(options);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(@lock.ETag));
        }
    }
}