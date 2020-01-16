using System;
using System.Net;
using System.Threading.Tasks;
using System.Timers;

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
         * <param name="client">The Cosmos DB client object.</param>
         * <param name="databaseName">The name of the database that contains the lease container.</param>
         * <param name="containerName">The name of the lease container.</param>
         * <exception cref="ConsistencyLevelException">If the consistency level is anything other than Strong.</exception>
         */
        public LockClient(CosmosClient client, string databaseName, string containerName)
        {
            if (client == null) throw new ArgumentNullException(string.Format(_argumentExceptionMessage, nameof(client)));
            if (string.IsNullOrWhiteSpace(databaseName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(databaseName)));
            if (string.IsNullOrWhiteSpace(containerName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(containerName)));

            // Make sure the client suppports strong consistancy. Cosmos DB doesn't allow the consistency to be higher on a
            // client than what is defined on the subscription, so we can't enforce the consistancy to be strong.  But we can
            // check the consistancy on the account and fail if it isn't Strong.
            // TODO: should network requests be made from the constructor? Or should consistency failures occur when cosmos 
            // is queried elsewhere?
            CheckConsitencyLevel(client);

            _container = client.GetContainer(databaseName, containerName);
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
        public async Task<Lock> AcquireAsync(AcquireLockOptions options)
        {
            if (options == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(options)));
            if (string.IsNullOrWhiteSpace(options.PartitionKey)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(options.PartitionKey)));
            if (string.IsNullOrWhiteSpace(options.LockName)) throw new ArgumentException(string.Format(_argumentExceptionMessage, nameof(options.LockName)));
            if (options.TimeoutMS < 0) throw new ArgumentException("TimeoutMS must be greater than zero.");

            bool done = false;
            Exception innerEx = null;
            DateTime now = LockUtils.Now;
            while (!done)
            {
                try
                {
                    Lock @lock = await TryAcquireOnceAsync(options);
                    if (options.AutoRenew)
                    {
                        LaunchAutoRenewTimer(@lock);
                    }
                    return @lock;
                }
                catch (LockUnavailableException ex)
                {
                    innerEx = ex;
                }

                if ((LockUtils.Now - now).TotalMilliseconds < options.TimeoutMS)
                {
                    await Task.Delay(options.RetryWaitMS);
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
        public async Task RenewAsync(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(@lock)));

            try
            {
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                DateTime timeAcquired = LockUtils.Now;
                ItemResponse<Lock> response = await _container.ReplaceItemAsync(@lock, @lock.Name, null, options);
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
         * Releases the lock. If the lock does not exist, this will be a no-op.
         * </summary>
         * 
         * <param name="lock">The lock to release.</param>
         */
        public async Task ReleaseAsync(Lock @lock)
        {
            if (@lock == null) throw new ArgumentNullException(string.Format(_argumentNullExceptionMessage, nameof(@lock)));

            try
            {
                ItemRequestOptions options = new ItemRequestOptions() { IfMatchEtag = @lock.ETag };
                await _container.DeleteItemAsync<Lock>(@lock.Name, new PartitionKey(@lock.PartitionKey), options);
                @lock.IsAquired = false;
                if (@lock.AutoRenewTimer != null)
                {
                    @lock.AutoRenewTimer.Stop();
                    @lock.AutoRenewTimer = null;
                }
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
        private async Task<Lock> TryAcquireOnceAsync(AcquireLockOptions options)
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
                ItemResponse<Lock> response = await _container.CreateItemAsync(@lock);
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

        /**
         * <summary>
         * Checks the consistency level on the client & account level.  For locking to work properly,
         * a consistency level of Strong is needed.  There is no way currently to enforce a Strong consistency
         * level programmatically considering a client can only have an equal or lower level of consistency
         * than what is defined on the account.
         * </summary>
         * 
         * <param name="client">The Cosmos DB client to query account consistency against.</param>
         * <exception cref="ConsistencyLevelException">If the consistency level is anything other than Strong.</exception>
         */
        private void CheckConsitencyLevel(CosmosClient client)
        {
            bool strong = true;
            AccountProperties properties = client.ReadAccountAsync().Result;
            ConsistencyLevel level = client.ClientOptions.ConsistencyLevel ?? properties.Consistency.DefaultConsistencyLevel;
            Exception innerEx = null;
            try
            {
                if (level != ConsistencyLevel.Strong)
                {
                    strong = false;
                }
            }
            catch (AggregateException ex)
            {
                // If the client's consistency level is greater than that of the account's, ReadAccountAsync will 
                // throw an AggregateException.  Unfortuneately, there is no error code to confirm exac
                innerEx = ex;
                if (ex.InnerException != null && ex.InnerException is ArgumentException)
                {
                    strong = false;
                }
            }

            if (!strong)
            {
                throw new ConsistencyLevelException(level, innerEx);
            }
        }

        /**
         * <summary>
         * Creates and launchs a timer which will periodically renew the lease 
         * on the given lock.
         * </summary>
         * 
         * <param name="lock">The lock to automatically renew.</param>
         */
        private void LaunchAutoRenewTimer(Lock @lock)
        {
            double third = @lock.LeaseDuration * 1000 / 3.0;
            Timer timer = new Timer(third) { AutoReset = false };
            timer.Elapsed += async (sender, args) =>
            {
                DateTime start = LockUtils.Now;
                if (@lock.IsAquired)
                {
                    Console.WriteLine("IsAquired");
                    try
                    {
                        await RenewAsync(@lock);
                    }
                    catch
                    {
                        // Try again momentarily.
                    }

                    TimeSpan elapsed = LockUtils.Now - start;
                    timer.Interval = Math.Max(0.1, third - elapsed.TotalMilliseconds);
                    timer.Start();
                }
                else
                {
                    @lock.AutoRenewTimer = null;
                }
            };
            @lock.AutoRenewTimer = timer;
            timer.Start();
        }
    }
}