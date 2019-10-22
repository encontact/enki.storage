using enki.storage.Interface;
using enki.storage.Model;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    public class MinioStorageTest
    {
        private IStorageServerConfig _config { get; set; }
        public MinioStorageTest() => _config = StorageTestConfig.GetAppsettingsConfig(StorageType.Minio);

        [Fact]
        public async Task NotFoundBucketExistsAsyncTest()
        {
            var bucket = _config.DefaultBucket + "-notfoundbucket";
            try
            {
                var client = new MinioStorage(_config);
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
            }
        }

        [Fact]
        public async Task TestCreateBucket()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-createdbucket";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
            }
            finally
            {
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task SetCorsToCreatedBucketTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-cors";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.SetCorsToBucketAsync(bucket, "*").ConfigureAwait(false);
                Assert.True(true); // Se chegou aqui e não deu erro, está ok.
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
            }
            finally
            {
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task TestDeleteBucket()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-deletebucket";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
            }
        }

        [Fact]
        public async Task PutObjectTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-putobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task PresignPutObjectTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-presignputobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60).ConfigureAwait(false);

                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
                httpRequest.Method = "PUT";
                using (var dataStream = httpRequest.GetRequestStream())
                {
                    var buffer = new byte[8000];
                    using (var fileStream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                    {
                        var bytesRead = 0;
                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            dataStream.Write(buffer, 0, bytesRead);
                        }
                    }
                }
                var response = httpRequest.GetResponse() as HttpWebResponse;
                if (response.StatusCode != HttpStatusCode.OK) Assert.True(false, "Envio HTTP não acusou sucesso.");
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task GetObjectTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-getobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                using (var dataStream = new MemoryStream())
                {
                    await client.GetObjectAsync(bucket, bucketObject,
                        (stream) =>
                        {
                            stream.CopyTo(dataStream);
                        }
                    );
                    Assert.Equal(filePutSize, dataStream.Length);
                }
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task PresignGetObjectTest()
        {
            var client = new MinioStorage(_config);
            var fileName = "SimpleFile.txt";
            var bucketObject = $"test/{fileName}";
            var bucket = _config.DefaultBucket + "-presign-getobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                var urlToGetObject = await client.PresignedGetObjectAsync(bucket, bucketObject, 60).ConfigureAwait(false);
                Assert.Contains(fileName, urlToGetObject);
                using (var clientWeb = new WebClient())
                {
                    var data = await clientWeb.DownloadDataTaskAsync(new Uri(urlToGetObject)).ConfigureAwait(false);
                    Assert.Equal(filePutSize, data.Length);
                }
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
            }
            finally
            {
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
        }

        [Fact]
        public async Task CopyObjectTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var destBucketObject = "test/SimpleCopiedFile.txt";
            var bucket = _config.DefaultBucket + "-copy-object";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                await client.CopyObjectAsync(bucket, bucketObject, bucket, destBucketObject).ConfigureAwait(false);
                Assert.True(await client.ObjectExistAsync(bucket, destBucketObject).ConfigureAwait(false));
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveObjectAsync(bucket, destBucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task GetObjectInfoAsyncTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-stat-object";
            try
            {
                // Prepare
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));

                // Test
                var data = await client.GetObjectInfoAsync(bucket, bucketObject).ConfigureAwait(false);
                Assert.NotNull(data);
                Assert.NotNull(data.ETag);
                Assert.True(data.ETag.Count() > 10);
                Assert.Equal("text/plain", data.ContentType);
                Assert.Equal(DateTime.MaxValue, data.Expires);
                Assert.Equal(DateTime.Now.Year, data.LastModified.Year);
                Assert.Equal(DateTime.Now.Month, data.LastModified.Month);
                Assert.Equal(DateTime.Now.Day, data.LastModified.Day);
                Assert.Equal(bucketObject, data.ObjectName);
                Assert.Equal(54, data.Size);
                Assert.Equal(1, data.MetaData.Count);
                Assert.Equal("text/plain", data.MetaData["Content-Type"]);

                // Clean
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Assert.False(true, e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
            }
        }
    }
}
