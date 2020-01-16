using Microsoft.Azure.Cosmos;
using Moq;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace cosmosdb_lock_client_test
{
    public class MockCosmosClient
    {
        Mock<CosmosClient> _mockClient = new Mock<CosmosClient>();

        public CosmosClient Client
        {
            get { return _mockClient.Object; }
        }

        public ConsistencyLevel AccountConsistencyLevel { get; set; } = ConsistencyLevel.Strong;
        public ConsistencyLevel? ClientConsistencyLevel { get; set; } = ConsistencyLevel.Strong;
        public MockContainer MockContainer { get; private set; } = new MockContainer();

        public MockCosmosClient()
        {
            _mockClient
                .Setup(x => x.ClientOptions)
                .Returns(() => new CosmosClientOptions() { ConsistencyLevel = ClientConsistencyLevel });
            _mockClient
                .Setup(x => x.GetContainer(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((d, c) => MockContainer.Container);
            _mockClient
                .Setup(x => x.ReadAccountAsync())
                .Returns(ReadAccountAsync);
        }

        private Task<AccountProperties> ReadAccountAsync()
        {
            if (AccountConsistencyLevel == ConsistencyLevel.Strong)
            {
                // Note that Moq can't be used on AccountProperties because its not abstract nor virtual.
                // So fake a mock via JSON deserialization.
                string json = $"{{ \"userConsistencyPolicy\": {{ \"DefaultConsistencyPolicy\": \"Strong\" }} }}";
                return Task.FromResult(JsonConvert.DeserializeObject<AccountProperties>(json));
            }
            throw new ConsistencyLevelException(AccountConsistencyLevel);
        }
    }
}
