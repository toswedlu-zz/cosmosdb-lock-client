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
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            MockContainer mockContainer = new MockContainer();
            LockClient client = new LockClient(mockContainer.Container);
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
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            MockContainer mockContainer = new MockContainer();
            LockClient client = new LockClient(mockContainer.Container);
            Lock @lock = client.Acquire(options);
            Thread.Sleep(1100);
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
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 120
            };
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
            Lock @lock = lockClient.Acquire(options);
            lockClient.Release(@lock);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public void LockHasValue()
        {
            MockContainer mockContainer = new MockContainer();
            LockClient lockClient = new LockClient(mockContainer.Container);
            Assert.ThrowsException<ArgumentNullException>(() => lockClient.Release(null));
        }

        [TestMethod]
        public void CosmosExceptionThrown()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };

            // Note: for the cosmos exception to be thrown, the status code needs to be anything but PreconditionFailed.
            CosmosException innerEx = new CosmosException(string.Empty, HttpStatusCode.OK, 0, string.Empty, 0);
            MockContainer mockContainer = new MockContainer() { ExceptionToThrowOnRelease = new AggregateException(innerEx) };
            LockClient client = new LockClient(mockContainer.Container);
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<AggregateException>(() => client.Release(@lock));
        }

        [TestMethod]
        public void OtherExceptionThrown()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            MockContainer mockContainer = new MockContainer() { ExceptionToThrowOnRelease = new Exception() };
            LockClient client = new LockClient(mockContainer.Container);
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<Exception>(() => client.Release(@lock));
        }
    }
}
