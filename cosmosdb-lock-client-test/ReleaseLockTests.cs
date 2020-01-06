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
                LeaseDurationMS = 120000
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
                LeaseDurationMS = 1000
            };
            MockContainer mockContainer = new MockContainer();
            LockClient client = new LockClient(mockContainer.Container);
            Lock @lock = client.Acquire(options);
            Thread.Sleep(1100);
            try
            {
                client.Release(@lock);
                // TODO: make sure the lock is actually release as well.
            }
            catch (AggregateException ex)
            {
                CosmosException innerEx = ex.InnerException as CosmosException;
                if (innerEx != null && innerEx.StatusCode == HttpStatusCode.PreconditionFailed)
                {
                    Assert.Fail();
                }
            }
        }

        [TestMethod]
        public void CosmosExceptionThrown()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name"
            };
            MockContainer mockContainer = new MockContainer() { ExceptionToThrowOnRelease = new CosmosException(string.Empty, HttpStatusCode.OK, 0, string.Empty, 0) };
            LockClient client = new LockClient(mockContainer.Container);
            Lock @lock = client.Acquire(options);
            Assert.ThrowsException<CosmosException>(() => client.Release(@lock));
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
