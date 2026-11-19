using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.S3;
using enki.storage.Interface;
using enki.storage.Model;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    /// <summary>
    /// PROD-3314 — S3 Transfer Acceleration por cliente. Só as URLs assinadas (upload e download) podem
    /// sair no domínio acelerado {bucket}.s3-accelerate.amazonaws.com; o padrão continua desligado e as
    /// operações do servidor continuam no endpoint configurado. Sem container: assinar URL não acessa a rede.
    /// </summary>
    public class AwsS3StorageAccelerateTest
    {
        private const string AwsEndpoint = "https://s3.amazonaws.com";
        private const string Bucket = "test-bucket";
        private const string Key = "some/key.txt";
        private const string AcceleratedPrefix = "https://test-bucket.s3-accelerate.amazonaws.com/some/key.txt";
        private const string StandardPrefix = "https://s3.amazonaws.com/test-bucket/some/key.txt";

        private sealed class RegionSelectionConfig : IStorageServerConfig
        {
            public string EndPoint => AwsEndpoint;
            public string AccessKey => "access-key";
            public string SecretKey => "secret-key";
            public string Region { get; set; } = "us-west-2";
            public bool Secure => true;
            public string DefaultBucket => Bucket;
            public bool ConnectToRegion { get; set; }
            public bool MustConnectToRegion() => ConnectToRegion;
        }

        private static AwsS3Storage Connect(string region = "us-east-1")
        {
            var storage = new AwsS3Storage(new StorageConfigTest
            {
                EndPoint = AwsEndpoint,
                AccessKey = "access-key",
                SecretKey = "secret-key",
                Secure = true,
                DefaultBucket = Bucket,
                Region = region
            });
            storage.Connect();
            return storage;
        }

        private static AmazonS3Config MainClientConfig(AwsS3Storage storage)
        {
            var property = typeof(AwsS3Storage).GetProperty("_client", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(property);
            var client = (IAmazonS3)property.GetValue(storage);
            return (AmazonS3Config)client.Config;
        }

        private static string[] QueryParameters(string url)
            => new Uri(url).Query.TrimStart('?').Split('&');

        private static string[] CredentialScope(string url)
        {
            var credential = Assert.Single(QueryParameters(url), p => p.StartsWith("X-Amz-Credential=", StringComparison.Ordinal));
            return Uri.UnescapeDataString(credential.Substring("X-Amz-Credential=".Length)).Split('/');
        }

        [Fact]
        public async Task AcceleratedPresignedGetMustUseAccelerateDomainWithBucketInHost()
        {
            var storage = Connect();

            var url = await storage.PresignedGetObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);

            Assert.StartsWith(AcceleratedPrefix, url);
            Assert.Equal("test-bucket.s3-accelerate.amazonaws.com", new Uri(url).Host);
        }

        [Fact]
        public async Task AcceleratedPresignedPutMustUseAccelerateDomainWithBucketInHost()
        {
            var storage = Connect();

            var url = await storage.PresignedPutObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);

            Assert.StartsWith(AcceleratedPrefix, url);
        }

        [Fact]
        public async Task AcceleratedPresignedPutMustKeepContentMd5Signed()
        {
            var storage = Connect();
            const string md5 = "1B2M2Y8AsgTpgAmY7PhCfg==";

            var url = await storage.PresignedPutObjectAsync(Bucket, Key, 600, md5, useAccelerateEndpoint: true);

            Assert.StartsWith(AcceleratedPrefix, url);
            var signedHeaders = Assert.Single(QueryParameters(url), p => p.StartsWith("X-Amz-SignedHeaders=", StringComparison.Ordinal));
            Assert.Contains("content-md5", Uri.UnescapeDataString(signedHeaders));
        }

        [Fact]
        public async Task AcceleratedPresignedGetMustKeepResponseHeaderOverrides()
        {
            var storage = Connect();
            var reqParams = new System.Collections.Generic.Dictionary<string, string>
            {
                { "response-content-disposition", "inline;filename=\"a.pdf\";" },
                { "response-content-type", "application/pdf" }
            };

            var url = await storage.PresignedGetObjectAsync(Bucket, Key, 600, reqParams, useAccelerateEndpoint: true);

            Assert.StartsWith(AcceleratedPrefix, url);
            Assert.Contains("response-content-type=application%2Fpdf", url);
            Assert.Contains("response-content-disposition=", url);
        }

        [Fact]
        public async Task DefaultMustStayOnConfiguredEndpoint()
        {
            var storage = Connect();

            var getUrl = await storage.PresignedGetObjectAsync(Bucket, Key, 600);
            var putUrl = await storage.PresignedPutObjectAsync(Bucket, Key, 600);
            var getFalse = await storage.PresignedGetObjectAsync(Bucket, Key, 600, null, false);
            var putFalse = await storage.PresignedPutObjectAsync(Bucket, Key, 600, null, false);

            foreach (var url in new[] { getUrl, putUrl, getFalse, putFalse })
            {
                Assert.StartsWith(StandardPrefix, url);
                Assert.DoesNotContain("s3-accelerate", url);
            }
        }

        [Fact]
        public async Task AcceleratedUrlMustNotLeakIntoLaterStandardUrlsNorServerClient()
        {
            var storage = Connect();

            await storage.PresignedPutObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);
            await storage.PresignedGetObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);

            // Operações do servidor (gravar, copiar, apagar, verificar existência) usam o cliente principal:
            // ele tem que continuar no endpoint configurado, com o bucket no caminho.
            var config = MainClientConfig(storage);
            Assert.False(config.UseAccelerateEndpoint);
            Assert.True(config.ForcePathStyle);
            Assert.Equal(AwsEndpoint, config.ServiceURL.TrimEnd('/'));

            Assert.StartsWith(StandardPrefix, await storage.PresignedGetObjectAsync(Bucket, Key, 600));
            Assert.StartsWith(StandardPrefix, await storage.PresignedPutObjectAsync(Bucket, Key, 600));
        }

        [Theory]
        [InlineData(false, "us-east-1")]
        [InlineData(true, "us-west-2")]
        public async Task AcceleratedUrlsMustBeSignedForTheConfiguredRegion(bool connectToRegion, string expectedRegion)
        {
            var storage = new AwsS3Storage(new RegionSelectionConfig { ConnectToRegion = connectToRegion });
            storage.Connect();

            var getUrl = await storage.PresignedGetObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);
            var putUrl = await storage.PresignedPutObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);

            foreach (var url in new[] { getUrl, putUrl })
            {
                Assert.StartsWith(AcceleratedPrefix, url);
                var scope = CredentialScope(url);
                Assert.Equal(5, scope.Length);
                Assert.Equal(expectedRegion, scope[2]);
                Assert.Equal("s3", scope[3]);
            }
        }

        [Fact]
        public async Task StandardAndAcceleratedSignaturesMustDiffer()
        {
            // A assinatura cobre o host: reaproveitar a URL padrão trocando só o domínio não funcionaria.
            var storage = Connect();

            var standard = await storage.PresignedGetObjectAsync(Bucket, Key, 600);
            var accelerated = await storage.PresignedGetObjectAsync(Bucket, Key, 600, useAccelerateEndpoint: true);

            string Signature(string u) => QueryParameters(u).Single(p => p.StartsWith("X-Amz-Signature=", StringComparison.Ordinal));
            Assert.NotEqual(Signature(standard), Signature(accelerated));
        }
    }
}
