using System.Threading.Tasks;
using Testcontainers.LocalStack;
using Xunit;

namespace enki.storage.integration.test.Infrastructure.Containers
{
    public class LocalStackContainerFixture : IAsyncLifetime
    {
        private readonly LocalStackContainer container;

        public string Endpoint => container.GetConnectionString();
        public string AccessKey = "test";
        public string SecretKey = "test";
        public string Region = "us-east-1";

        public LocalStackContainerFixture()
        {
            container = new LocalStackBuilder("localstack/localstack:latest")
                .WithPortBinding("4566")
                .WithEnvironment("SERVICES", "s3")
                .WithEnvironment("AWS_ACCESS_KEY_ID", "test")
                .WithEnvironment("AWS_SECRET_ACCESS_KEY", "test")
                .WithEnvironment("AWS_DEFAULT_REGION", "us - east - 1")
                .WithEnvironment("DOCKER_HOST", "unix:///var/run/docker.sock")
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
