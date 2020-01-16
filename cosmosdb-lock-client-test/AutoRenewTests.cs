using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;

namespace cosmosdb_lock_client_test
{
    [TestClass]
    public class AutoRenewTests
    {
        [TestMethod]
        public async Task RenewNeverCalled()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                AutoRenew = false,
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            Assert.AreEqual(0, mockCosmosClient.MockContainer.ReplaceItemCallCount);
        }

        [TestMethod]
        public async Task RenewCalled()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                AutoRenew = true,
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 2000);
            Assert.AreEqual(5, mockCosmosClient.MockContainer.ReplaceItemCallCount);
            Assert.IsTrue(@lock.IsAquired);
            await lockClient.ReleaseAsync(@lock);
        }

        [TestMethod]
        public async Task ElapsesOnExceptions()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            mockCosmosClient.MockContainer.ExceptionToThrowOnRenew = new Exception();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                AutoRenew = true,
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            Assert.AreEqual(2, mockCosmosClient.MockContainer.ReplaceItemCallCount);
            Assert.IsFalse(@lock.IsAquired);
        }

        [TestMethod]
        public async Task TimerStoppedAfterRelease()
        {
            MockCosmosClient mockCosmosClient = new MockCosmosClient();
            LockClient lockClient = new LockClient(mockCosmosClient.Client, "dbname", "containername");
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                AutoRenew = true,
                LeaseDuration = 2
            };
            Lock @lock = await lockClient.AcquireAsync(options);
            await Task.Delay(options.LeaseDuration * 1000);
            await lockClient.ReleaseAsync(@lock);
            await Task.Delay(options.LeaseDuration * 1000);
            Assert.AreEqual(2, mockCosmosClient.MockContainer.ReplaceItemCallCount);
        }
    }
}
