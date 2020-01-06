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
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 2000,
                RetryWaitMS = 1000
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
            lockClient.Acquire(options);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(4, mockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public void FailWithZeroTimeout()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 0
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
            lockClient.Acquire(options);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(2, mockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public void WithExpiredLock()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDurationMS = 1000
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
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
        public void ParitionKeyHasValue()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = string.Empty,
                LockName = "test-name"
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
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
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = string.Empty
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
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
        public void ETagAssigned()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock"
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
            Lock @lock = lockClient.Acquire(options);
            Assert.IsTrue(!string.IsNullOrWhiteSpace(@lock.ETag));
        }
    }
}