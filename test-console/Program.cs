using System;
using Microsoft.Azure.Cosmos;

namespace test_console
{
    class Program
    {
        static void Main(string[] args)
        {
            string connStr = "AccountEndpoint=https://tom-sql-test.documents.azure.com:443/;AccountKey=yOK8rYingKwQOv8GXcpnLQkjRhs0NuMkqz40WWQVwwpFMh6tB4EKUDrxudTllJhSPQAr5NH4VXz962Ueg0IUUQ==;";
            CosmosClient cosmosClient = new CosmosClient(connStr);
            Container container = cosmosClient.GetContainer("test-lease", "lease-container");


            AcquireLockOptions options = new AcquireLockOptions()
            {
                PartitionKey = "test-key",
                LockName = "test-lock",
                LeaseDurationMS = 600000
            };
            LockClient lockClient = new LockClient(container);
            Lock @lock = lockClient.Acquire(options);
            ItemRequestOptions op = new ItemRequestOptions()
            {
                IfMatchEtag = Guid.NewGuid().ToString()//@lock.ETag
            };
            ItemResponse<Lock> response = container.DeleteItemAsync<Lock>("test-lock", new PartitionKey("test-key"), op).Result;


            //LockClient lockClient = new LockClient(cosmosClient.GetContainer("test-lease", "lease-container"));

            //cosmosClient.GetContainer

            //try
            //{
            //    AcquireLockOptions options = new AcquireLockOptions()
            //    {
            //        PartitionKey = "pk1",
            //        LockName = "lock1"
            //    };
            //    Lock @lock = lockClient.Acquire(options);
            //}
            //catch (Exception ex)
            //{

            //}
        }
    }
}
