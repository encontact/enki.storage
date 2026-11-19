using Xunit;
using enki.storage.integration.test.Infrastructure.Containers;

namespace enki.storage.integration.test.Infrastructure.Collections
{
    [CollectionDefinition("SeaweedFs collection")]
    public class SeaweedFsCollection : ICollectionFixture<SeaweedFsContainerFixture>
    {
    }
}
