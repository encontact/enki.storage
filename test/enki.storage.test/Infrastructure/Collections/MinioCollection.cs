using Xunit;
using enki.storage.integration.test.Infrastructure.Containers;

namespace enki.storage.integration.test.Infrastructure.Collections
{
    [CollectionDefinition("Minio collection")]
    public class MinioCollection : ICollectionFixture<MinioContainerFixture>
    {
    }
}
