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

        public Container Container
        {
            get { return _mockContainer.Object; }
        }

        public Exception ExceptionToThrowOnRelease { get; set; }

        public int CreateItemCallCount { get; private set; }

        public MockContainer()
        {
            _mockContainer
                .Setup(x => x.CreateItemAsync(It.IsAny<Lock>(), null, null, default))
                .Returns<Lock, PartitionKey, ItemRequestOptions, CancellationToken>((l, pk, op, t) =>
                {
                    CreateItemCallCount++;
                    RemoveLockIfExpired(l);
                    if (_locks.ContainsKey(l.Name))
                    {
                        CosmosException ex = new CosmosException(string.Empty, HttpStatusCode.Conflict, 0, string.Empty, 0);
                        throw new AggregateException(string.Empty, ex);
                    }
                    else
                    {
                        string etag = Guid.NewGuid().ToString();
                        MockLeaseItem lease = new MockLeaseItem() { Lock = l, ETag = etag, TimeAquired = DateTime.Now };
                        _locks.Add(l.Name, lease);
                        ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.Created, etag);
                        return Task.FromResult(response);
                    }
                });
            _mockContainer
                .Setup(x => x.DeleteItemAsync<Lock>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), default))
                .Returns<string, PartitionKey, ItemRequestOptions, CancellationToken>((s, pk, op, t) =>
                {
                    if (ExceptionToThrowOnRelease != null)
                    {
                        throw ExceptionToThrowOnRelease;
                    }

                    if (_locks.ContainsKey(s))
                    {
                        ItemResponse<Lock> response = CreateMockItemResponse(HttpStatusCode.NoContent, _locks[s].ETag);
                        if (_locks[s].ETag == op.IfMatchEtag)
                        {
                            _locks.Remove(s);
                            return Task.FromResult(response);
                        }
                        else
                        {
                            CosmosException ex = new CosmosException(string.Empty, HttpStatusCode.PreconditionFailed, 0, string.Empty, 0);
                            throw new AggregateException(string.Empty, ex);
                        }
                    }
                    else
                    {
                        CosmosException ex = new CosmosException(string.Empty, HttpStatusCode.NotFound, 0, string.Empty, 0);
                        throw new AggregateException(string.Empty, ex);
                    }
                });
        }

        private void RemoveLockIfExpired(Lock @lock)
        {
            if (_locks.ContainsKey(@lock.Name))
            {
                int leaseDurationMS = @lock.LeaseDuration * 1000;
                TimeSpan diff = DateTime.Now - _locks[@lock.Name].TimeAquired;
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
