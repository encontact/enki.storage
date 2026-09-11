using System;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using enki.storage.Interface;
using enki.storage.Model;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    /// <summary>
    /// Cobre a montagem do AmazonS3Config em Connect(). Não usa container porque nem Connect()
    /// nem a geração de URL pré-assinada produzem tráfego de rede.
    /// </summary>
    public class AwsS3StorageConnectionTest
    {
        private const string AwsEndpoint = "https://s3.amazonaws.com";
        private const string CompatibleEndpoint = "http://localhost:4566";

        private sealed class RegionSelectionConfig : IStorageServerConfig
        {
            public string EndPoint => AwsEndpoint;
            public string AccessKey => "access-key";
            public string SecretKey => "secret-key";
            public string Region { get; set; } = "us-west-2";
            public bool Secure => true;
            public string DefaultBucket => "test-bucket";
            public bool ConnectToRegion { get; set; }
            public bool MustConnectToRegion() => ConnectToRegion;
        }

        private static AwsS3Storage Connect(string endPoint, string region, bool secure)
        {
            var storage = new AwsS3Storage(new StorageConfigTest
            {
                EndPoint = endPoint,
                AccessKey = "access-key",
                SecretKey = "secret-key",
                Secure = secure,
                DefaultBucket = "test-bucket",
                Region = region
            });

            storage.Connect();

            return storage;
        }

        private static IClientConfig ConfigOf(AwsS3Storage storage)
        {
            var property = typeof(AwsS3Storage).GetProperty("_client", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(property);

            var client = (IAmazonS3)property.GetValue(storage);
            Assert.NotNull(client);

            return client.Config;
        }

        [Fact]
        public void AwsEndpointMustKeepServiceUrlAndAuthenticationRegion()
        {
            var config = ConfigOf(Connect(AwsEndpoint, "us-east-1", true));

            // Definir RegionEndpoint aqui limparia o ServiceURL e trocaria o host das requisições
            // pelo endpoint regional, inclusive nas URLs pré-assinadas entregues ao navegador.
            Assert.Null(config.RegionEndpoint);
            Assert.Equal(AwsEndpoint, config.ServiceURL.TrimEnd('/'));
            Assert.Equal("us-east-1", config.AuthenticationRegion);
        }

        [Fact]
        public void CompatibleEndpointMustKeepServiceUrlAndAuthenticationRegion()
        {
            var config = ConfigOf(Connect(CompatibleEndpoint, "us-east-1", false));

            Assert.Null(config.RegionEndpoint);
            Assert.Equal(CompatibleEndpoint, config.ServiceURL.TrimEnd('/'));
            Assert.Equal("us-east-1", config.AuthenticationRegion);
        }

        [Fact]
        public void EndpointWithoutConfiguredRegionMustStillResolveAuthenticationRegion()
        {
            var config = ConfigOf(Connect(AwsEndpoint, null, true));

            // Sem AuthenticationRegion o SDK volta a inferir a região por expressão regular a cada
            // operação, que é exatamente o custo que esta correção remove.
            Assert.Null(config.RegionEndpoint);
            Assert.Equal(AwsEndpoint, config.ServiceURL.TrimEnd('/'));
            Assert.False(string.IsNullOrWhiteSpace(config.AuthenticationRegion));
        }

        [Fact]
        public async Task PresignedUrlMustUseConfiguredEndpointHost()
        {
            var storage = Connect(AwsEndpoint, "us-east-1", true);

            var url = await storage.PresignedGetObjectAsync("test-bucket", "some/key.txt", 600);

            Assert.StartsWith("https://s3.amazonaws.com/test-bucket/some/key.txt", url);
        }

        [Theory]
        [InlineData(false, "us-east-1")]
        [InlineData(true, "us-west-2")]
        public async Task PresignedUrlsMustRespectRegionSelection(bool connectToRegion, string expectedRegion)
        {
            var storage = new AwsS3Storage(new RegionSelectionConfig { ConnectToRegion = connectToRegion });
            storage.Connect();

            var config = ConfigOf(storage);
            Assert.Null(config.RegionEndpoint);
            Assert.Equal(AwsEndpoint, config.ServiceURL.TrimEnd('/'));
            Assert.Equal(expectedRegion, config.AuthenticationRegion);

            var getUrl = await storage.PresignedGetObjectAsync("test-bucket", "some/key.txt", 600);
            var putUrl = await storage.PresignedPutObjectAsync("test-bucket", "some/key.txt", 600);

            foreach (var url in new[] { getUrl, putUrl })
            {
                Assert.StartsWith("https://s3.amazonaws.com/test-bucket/some/key.txt", url);
                var credential = Assert.Single(new Uri(url).Query.TrimStart('?').Split('&'),
                    parameter => parameter.StartsWith("X-Amz-Credential=", StringComparison.Ordinal));
                var scope = Uri.UnescapeDataString(credential.Substring("X-Amz-Credential=".Length)).Split('/');
                Assert.Equal(5, scope.Length);
                Assert.Equal(expectedRegion, scope[2]);
                Assert.Equal("s3", scope[3]);
                Assert.Equal("aws4_request", scope[4]);
            }
        }
    }
}
