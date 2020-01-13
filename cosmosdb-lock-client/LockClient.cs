using System;
using System.Net;
using System.Threading;

namespace Microsoft.Azure.Cosmos
{
    /**
     * <summary>
     * A simple locking library built on top of Cosmos DB for managing distributed locks.
     * </summary>
     */
    public class LockClient
    {
        static string _argumentExceptionMessage = "{0} must have a non-empty, non-null value.";
        static string _argumentNullExceptionMessage = "{0} must be non-null.";

        Container _container;

        /**
         * <summary>
         * Instantiates a new lock client with the given Cosmos DB container.
         * </summary>
         * 
         * <param name="leaseContainer"></param>
         */
        public LockClient(Container leaseContainer)
        {
            _container = leaseContainer;
        }

        /**
         * <summary>
         * Attempts to acquire a lock with the given options.  If the lock is unavailable, this will 
         * retry for the specified amount of time until either acquiring the lock or giving up.
         * </summary>
         * 
         * <param name="options">The options used to configure how the lock is acquired.</param>
         * <returns>A <c>Lock</c> object representing the acquired lock.</returns>
         * <exception cref="LockUnavailableException">
         * If the lock is unable to be acquired within the given timeout.
         * </exception>
         */
        public Lock Acquire(AcquireLockOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.PartitionKey)) throw new ArgumentException(string.Format(_argumentExceptionMessage, "PartitionKey"));
            if (string.IsNullOrWhiteSpace(options.LockName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, "LockName"));
            if (options.TimeoutMS < 0) throw new ArgumentException("TimeoutMS must be greater than zero.");

            bool done = false;
            Exception innerEx = null;
            DateTime now = LockUtils.Now;
            while (!done)
            {
                try
                {
                    return TryAcquireOnce(options);
                }
                catch (LockUnavailableException ex)
                {
                    innerEx = ex;
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

            throw new LockUnavailableException(options.PartitionKey, options.LockName, innerEx);
        }

        /**
         * <summary>
         * Renews the lease on the given lock.  If the lock does not exist in Cosmos DB, then the lock
         * has been released/expired. If the lock exists in Cosmos DB, but the ETags don't match, then 
         * lock been released/expired and reacquired as is a different lock.
         * </summary>
         * 
         * <param name="lock">The lock to renew.</param>
         * <exception cref="LockReleasedException">
         * If the lock with the given name and ETag cannot be found in Cosmos DB.
         * </exception>
         */
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

        /**
         * <summary>
         * Releases the lock. If the lock does not exi
         * </summary>
         * 
         * <param name="lock">The lock to release.</param>
         */
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

        /**
         * <summary>
         * Trys to acquire a lock only once, without retring upon failure.
         * </summary>
         * 
         * <param name="options">The options used to configure how the lock is acquired.</param>
         * <returns>A <c>Lock</c> object representing the acquired lock.</returns>
         * <exception cref="LockUnavailableException">If the lock is unable to be acquired.</exception>
         */
        private Lock TryAcquireOnce(AcquireLockOptions options)
        {
            Lock @lock = new Lock()
            {
                Name = options.LockName,
                PartitionKey = options.PartitionKey,
                LeaseDuration = options.LeaseDuration,
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