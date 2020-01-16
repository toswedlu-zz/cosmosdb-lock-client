using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace cosmosdb_lock_client_test
{
    [TestClass]
    public class AcquireLockTests
    {
        [TestMethod]
        public async Task FailAfterTimeout()
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
            await lockClient.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<LockUnavailableException>(() => lockClient.AcquireAsync(options));
            Assert.AreEqual(4, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public async Task FailWithZeroTimeout()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                TimeoutMS = 0
            };
            await lockClient.AcquireAsync(options);
            await Assert.ThrowsExceptionAsync<LockUnavailableException>(() => lockClient.AcquireAsync(options));
            Assert.AreEqual(2, mockCosmosClient.MockContainer.CreateItemCallCount);
        }

        [TestMethod]
        public async Task WithExpiredLock()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 1
            };
            Thread.Sleep(options.LeaseDuration * 1000 + 100);
            try
            {
                await lockClient.AcquireAsync(options);
            }
            catch (LockUnavailableException)
            {
                Assert.Fail("Lock unavailable after expiration.");
            }
        }

        [TestMethod]
        public async Task IsAcquired()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-name",
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            Assert.IsTrue(@lock.IsAquired);
            Thread.Sleep(options.LeaseDuration * 1000);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public async Task ParitionKeyHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = string.Empty,
                LockName = "test-name"
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            options.PartitionKey = null;
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            try
            {
                options.PartitionKey = "test-key";
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task LockNameHasValue()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = string.Empty
            };
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

            options.LockName = null;
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => lockClient.AcquireAsync(options));

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
        public async Task OptionsHasValue()
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
                await lockClient.AcquireAsync(options);
            }
            catch
            {
                Assert.Fail();
            }

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => lockClient.AcquireAsync(null));
        }
    }
}