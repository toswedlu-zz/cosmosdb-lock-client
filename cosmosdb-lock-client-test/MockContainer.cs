using Microsoft.Azure.Cosmos;
using Moq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace cosmosdb_lock_client_test
{
    public class MockLeaseItem
    {
        public Lock Lock { get; set; }
        public string ETag { get; set; }
        public DateTime TimeAquired { get; set; }
    }

    public class MockContainer
    {
        Mock<Container> _mockContainer = new Mock<Container>();
        Dictionary<string, MockLeaseItem> _locks = new Dictionary<string, MockLeaseItem>();

        static DateTime Now { get { return DateTime.UtcNow; } }

        public Container Container
        {
            get { return _mockContainer.Object; }
        }

        public Exception ExceptionToThrowOnRelease { get; set; }
        public Exception ExceptionToThrowOnRenew { get; set; }
        public int CreateItemCallCount { get; private set; }

        public MockContainer()
        {
            _mockContainer
                .Setup(x => x.CreateItemAsync(It.IsAny<Lock>(), null, null, default))
                .Returns<Lock, PartitionKey, ItemRequestOptions, CancellationToken>(CreateItemAsync);
            _mockContainer
                .Setup(x => x.DeleteItemAsync<Lock>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), default))
                .Returns<string, PartitionKey, ItemRequestOptions, CancellationToken>(DeleteItemAsync);
            _mockContainer
                .Setup(x => x.ReplaceItemAsync<Lock>(It.IsAny<Lock>(), It.IsAny<string>(), null, It.IsAny<ItemRequestOptions>(), default))
                .Returns<Lock, string, PartitionKey, ItemRequestOptions, CancellationToken>(ReplaceItemAsync);
        }

        private Task<ItemResponse<Lock>> CreateItemAsync(Lock l, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            CreateItemCallCount++;
            RemoveLockIfExpired(l.Name);
            if (_locks.ContainsKey(l.Name))
            {
                CosmosException ex = new CosmosException(string.Empty, HttpStatusCode.Conflict, 0, string.Empty, 0);
                throw new AggregateException(string.Empty, ex);
            }
            else
            {
                string etag = Guid.NewGuid().ToString();
                MockLeaseItem lease = new MockLeaseItem() { Lock = l, ETag = etag, TimeAquired = Now };
                _locks.Add(l.Name, lease);
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.Created, etag);
                return Task.FromResult(response);
            }
        }

        private Task<ItemResponse<Lock>> DeleteItemAsync(string s, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            if (ExceptionToThrowOnRelease != null)
            {
                throw ExceptionToThrowOnRelease;
            }

            HttpStatusCode statusCode;
            RemoveLockIfExpired(s);
            if (_locks.ContainsKey(s))
            {
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.NoContent, _locks[s].ETag);
                if (!string.IsNullOrWhiteSpace(op.IfMatchEtag) && _locks[s].ETag == op.IfMatchEtag)
                {
                    _locks.Remove(s);
                    return Task.FromResult(response);
                }
                else
                {
                    statusCode = HttpStatusCode.PreconditionFailed;
                }
            }
            else
            {
                statusCode = HttpStatusCode.NotFound;
            }

            CosmosException ex = new CosmosException(string.Empty, statusCode, 0, string.Empty, 0);
            throw new AggregateException(string.Empty, ex);
        }

        private Task<ItemResponse<Lock>> ReplaceItemAsync(Lock l, string s, PartitionKey pk, ItemRequestOptions op, CancellationToken t)
        {
            if (ExceptionToThrowOnRenew != null)
            {
                throw ExceptionToThrowOnRenew;
            }

            HttpStatusCode statusCode;
            RemoveLockIfExpired(l.Name);
            if (_locks.ContainsKey(l.Name))
            {
                string etag = Guid.NewGuid().ToString();
                ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.OK, etag);
                if (!string.IsNullOrWhiteSpace(op.IfMatchEtag) && _locks[s].ETag == op.IfMatchEtag)
                {
                    _locks[s].Lock = l;
                    _locks[s].ETag = etag;
                    _locks[s].TimeAquired = Now;
                    return Task.FromResult(response);
                }
                else
                {
                    statusCode = HttpStatusCode.PreconditionFailed;
                }
            }
            else
            {
                statusCode = HttpStatusCode.NotFound;
            }

            CosmosException ex = new CosmosException(string.Empty, statusCode, 0, string.Empty, 0);
            throw new AggregateException(string.Empty, ex);
        }

        private void RemoveLockIfExpired(string name)
        {
            if (_locks.ContainsKey(name))
            {
                Lock @lock = _locks[name].Lock;
                int leaseDurationMS = @lock.LeaseDuration * 1000;
                TimeSpan diff = Now - _locks[@lock.Name].TimeAquired;
                if (_locks.ContainsKey(@lock.Name) && diff.TotalMilliseconds >= leaseDurationMS)
                {
                    _locks.Remove(@lock.Name);
                }
            }
        }

        private static ItemResponse<Lock> CreateMockItemResponse(HttpStatusCode statusCode, string etag)
        {
            Mock<ItemResponse<Lock>> mockResponse = new Mock<ItemResponse<Lock>>();
            mockResponse.Setup(x => x.StatusCode).Returns(statusCode);
            mockResponse.Setup(x => x.ETag).Returns(etag);
            return mockResponse.Object;
        }
    }
}
