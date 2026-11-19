using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using enki.storage.integration.test.Infrastructure.Containers;
using enki.storage.Interface;
using enki.storage.Model;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    /// <summary>
    /// PROD-3314 — no MinIO a opção de endpoint acelerado é ignorada: a URL continua no servidor
    /// configurado e o envio e a leitura pela URL assinada funcionam normalmente.
    /// </summary>
    [Collection("Minio collection")]
    public class MinioStorageAccelerateTest
    {
        private readonly IStorageServerConfig _config;

        public MinioStorageAccelerateTest(MinioContainerFixture fixture)
        {
            _config = StorageTestConfig.CreateMinio(fixture);
        }

        [Fact]
        public async Task AccelerateFlagMustBeIgnoredAndRoundTripMustWork()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/accelerate.txt";
            var bucket = _config.DefaultBucket + "-accelerate";
            var content = File.ReadAllBytes("resources/SimpleResourceToAttach.txt");
            client.Connect();
            await client.MakeBucketAsync(bucket);
            try
            {
                var putUrl = await client.PresignedPutObjectAsync(bucket, bucketObject, 60, null, useAccelerateEndpoint: true);
                Assert.DoesNotContain("s3-accelerate", putUrl);
                Assert.Equal(_config.EndPoint, new Uri(putUrl).Authority);

                using var http = new HttpClient();
                var put = await http.PutAsync(putUrl, new ByteArrayContent(content));
                Assert.Equal(HttpStatusCode.OK, put.StatusCode);
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));

                var getUrl = await client.PresignedGetObjectAsync(bucket, bucketObject, 60, null, useAccelerateEndpoint: true);
                Assert.DoesNotContain("s3-accelerate", getUrl);
                Assert.Equal(_config.EndPoint, new Uri(getUrl).Authority);

                var downloaded = await http.GetByteArrayAsync(getUrl);
                Assert.Equal(content, downloaded);
            }
            finally
            {
                if (await client.ObjectExistAsync(bucket, bucketObject))
                    await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
            }
        }
    }
}
