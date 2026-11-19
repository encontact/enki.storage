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
            // A MinIO Inc. tirou as imagens do Docker Hub e do quay.io (set/2026). A Chainguard é a única
            // origem pública e sem login; só publica :latest. Mesma imagem do encontact.app (TestContainerTuning).
            container = new MinioBuilder("cgr.dev/chainguard/minio:latest")
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
