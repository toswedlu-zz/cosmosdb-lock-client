using System;
using System.Net;
using System.Threading;

namespace Microsoft.Azure.Cosmos
{
    public class LockClient
    {
        static string _argumentExceptionMessage = "{0} must have a non-empty, non-null value.";
        static string _argumentNullExceptionMessage = "{0} must be non-null.";

        Container _container;

        public LockClient(Container leaseContainer)
        {
            _container = leaseContainer;
        }

        public Lock Acquire(AcquireLockOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.PartitionKey)) throw new ArgumentException(string.Format(_argumentExceptionMessage, "PartitionKey"));
            if (string.IsNullOrWhiteSpace(options.LockName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, "LockName"));
            if (options.TimeoutMS < 0) throw new ArgumentException("TimeoutMS must be greater than zero.");

            bool done = false;
            DateTime now = LockUtils.Now;
            while (!done)
            {
                try
                {
                    return TryAcquireOnce(options);
                }
                catch (LockUnavailableException)
                {
                    // Proceed to the code below.
                }

                if ((LockUtils.Now - now).TotalMilliseconds < options.TimeoutMS)
                {
                    Thread.Sleep(options.RetryWaitMS);
                }
                else
                {
                    done = true;
                }
            }

            throw new LockUnavailableException(options.PartitionKey, options.LockName);
        }

        public void Renew(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, "\"lock\""));

            try
            {
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                DateTime timeAcquired = LockUtils.Now;
                ItemResponse<Lock> response = _container.ReplaceItemAsync(@lock, @lock.Name, null, options).Result;
                @lock.TimeAcquired = timeAcquired;
                @lock.ETag = response.ETag;
            }
            catch (AggregateException ex)
            {
                CosmosException innerEx = ex.InnerException as CosmosException;
                if (innerEx != null && (innerEx.StatusCode == HttpStatusCode.PreconditionFailed || innerEx.StatusCode == HttpStatusCode.NotFound))
                {
                    throw new LockReleasedException(@lock, ex);
                }
                else
                {
                    throw;
                }
            }
        }

        public void Release(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, "\"lock\""));

            try
            {
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                _container.DeleteItemAsync<Lock>(@lock.Name, new PartitionKey(@lock.PartitionKey), options).Wait();
                @lock.IsAquired = false;
            }
            catch (AggregateException ex)
            {
                CosmosException innerEx = ex.InnerException as CosmosException;
                if (innerEx == null || (innerEx.StatusCode != HttpStatusCode.PreconditionFailed && innerEx.StatusCode != HttpStatusCode.NotFound))
                {
                    throw;
                }
            }
        }

        private Lock TryAcquireOnce(AcquireLockOptions options)
        {
            Lock @lock = new Lock()
            {
                Name = options.LockName,
                PartitionKey = options.PartitionKey,
                LeaseDuration = options.LeaseDurationMS / 1000,
            };

            try
            {
                @lock.TimeAcquired = LockUtils.Now;
                ItemResponse<Lock> response = _container.CreateItemAsync(@lock).Result;
                @lock.ETag = response.ETag;
                return @lock;
            }
            catch (AggregateException ex)
            {
                CosmosException innerEx = ex.InnerException as CosmosException;
                if (innerEx != null && innerEx.StatusCode == HttpStatusCode.Conflict)
                {
                    throw new LockUnavailableException(@lock, ex);
                }
                else
                {
                    throw;
                }
            }
        }
    }
}