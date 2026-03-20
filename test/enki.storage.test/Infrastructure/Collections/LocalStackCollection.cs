using Xunit;
using enki.storage.integration.test.Infrastructure.Containers;

namespace enki.storage.integration.test.Infrastructure.Collections
{
    [CollectionDefinition("LocalStack collection")]
    public class LocalStackCollection : ICollectionFixture<LocalStackContainerFixture>
    {
    }
}
