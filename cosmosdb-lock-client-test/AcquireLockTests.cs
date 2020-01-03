using Microsoft.Azure.Cosmos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Threading.Tasks;

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
            Mock<Container> mockContainer = CreateMockContainer();
            LockClient lockClient = new LockClient(mockContainer.Object);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(3, mockContainer.Invocations.Count);
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
            Mock<Container> mockContainer = CreateMockContainer();
            LockClient lockClient = new LockClient(mockContainer.Object);
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
            Assert.AreEqual(1, mockContainer.Invocations.Count);
        }

        [TestMethod]
        public void ParitionKeyHasValue()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = string.Empty,
                LockName = "test-name"
            };
            Mock<Container> mockContainer = CreateMockContainer();
            LockClient lockClient = new LockClient(mockContainer.Object);
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.PartitionKey = null;
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.PartitionKey = "test-key";
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
        }

        [TestMethod]
        public void LockNameHasValue()
        {
            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = string.Empty
            };
            Mock<Container> mockContainer = CreateMockContainer();
            LockClient lockClient = new LockClient(mockContainer.Object);
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.LockName = null;
            Assert.ThrowsException<ArgumentException>(() => lockClient.Acquire(options));

            options.LockName = "test-name";
            Assert.ThrowsException<LockUnavailableException>(() => lockClient.Acquire(options));
        }

        private Mock<Container> CreateMockContainer()
        {
            Mock<Container> mockContainer = new Mock<Container>();
            mockContainer
                .Setup(x => x.CreateItemAsync(It.IsAny<Lock>(), null, null, default))
                .Throws(new LockUnavailableException(string.Empty, string.Empty));
            return mockContainer;
        }
    }
}