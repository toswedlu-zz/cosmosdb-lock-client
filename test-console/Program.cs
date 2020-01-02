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
            LockClient lockClient = new LockClient(cosmosClient.GetContainer("test-lease", "lease-container"));

            try
            {
                AcquireLockOptions options = new AcquireLockOptions()
                {
                    PartitionKey = "pk1",
                    LockName = "lock1",
                };
                Lock @lock = lockClient.Aquire(options);
            }
            catch (Exception ex)
            {

            }
        }
    }
}
