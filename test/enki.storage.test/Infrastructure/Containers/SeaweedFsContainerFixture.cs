using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace enki.storage.integration.test.Infrastructure.Containers
{
    public class SeaweedFsContainerFixture : IAsyncLifetime
    {
        private const int S3Port = 8333;
        private readonly IContainer container;

        public string Endpoint => $"http://localhost:{container.GetMappedPublicPort(S3Port)}";
        public string AccessKey => "test";
        public string SecretKey => "test";
        public string Region => "us-east-1";

        public SeaweedFsContainerFixture()
        {
            // Não há módulo dedicado do Testcontainers pro SeaweedFS, por isso o container genérico.
            // O gateway S3 embutido (porta padrão 8333) aceita qualquer Access/Secret Key e implementa
            // o suficiente da API da AWS pra reusar o client do AwsS3Storage.
            container = new ContainerBuilder("chrislusf/seaweedfs:4.54")
                .WithPortBinding(S3Port, S3Port)
                .WithCommand("server", "-dir=/data", "-filer", "-s3", $"-s3.port={S3Port}", "-volume.max=0")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(S3Port).ForPath("/")))
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
