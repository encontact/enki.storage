using System.Threading.Tasks;
using Testcontainers.Minio;
using Xunit;

namespace enki.storage.integration.test.Infrastructure.Containers
{
    public class MinioContainerFixture : IAsyncLifetime
    {
        private readonly MinioContainer container;

        public string Endpoint => $"http://localhost:{container.GetMappedPublicPort(9000)}";
        public string AccessKey => container.GetAccessKey();
        public string SecretKey => container.GetSecretKey();

        public MinioContainerFixture()
        {
            container = new MinioBuilder("quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z")
                .WithReuse(true)
                .Build();
        }

        public async Task InitializeAsync()
        {
            await container.StartAsync();
        }

        public async Task DisposeAsync()
        {
            await container.DisposeAsync();
        }
    }
}
