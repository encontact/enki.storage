using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using enki.storage.Interface;
using enki.storage.Model;
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
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                await client.SetCorsToBucketAsync(bucket, "*");
                Assert.True(true); // Se chegou aqui e não deu erro, está ok.
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    var result = await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                    Assert.True(result.SuccessResult);
                }

                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60);

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
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
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
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                var urlToGetObject = await client.PresignedGetObjectAsync(bucket, bucketObject, 60);
                Assert.Contains(fileName, urlToGetObject);
                using (var clientWeb = new WebClient())
                {
                    var data = await clientWeb.DownloadDataTaskAsync(new Uri(urlToGetObject));
                    Assert.Equal(filePutSize, data.Length);
                }
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                await client.CopyObjectAsync(bucket, bucketObject, bucket, destBucketObject);
                Assert.True(await client.ObjectExistAsync(bucket, destBucketObject));
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveObjectAsync(bucket, destBucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
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
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));

                // Test
                var data = await client.GetObjectInfoAsync(bucket, bucketObject);
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
                Assert.Equal(2, data.MetaData.Count);
                Assert.Equal("text/plain", data.MetaData["Content-Type"]);

                // Clean
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task RemoveObjectTest()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-deleteobjects";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                await client.RemoveObjectAsync(bucket, bucketObject);
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task RemoveObjectsTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-putobject";
            var itemQuantity = 10;
            var objectList = new List<string>();
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    for (int i = 0; i < itemQuantity; i++)
                    {
                        var bucketObject = $"test/SimpleFile{i}.txt";
                        Assert.False(await client.ObjectExistAsync(bucket, bucketObject));

                        await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");

                        Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                        objectList.Add(bucketObject);
                    }
                }

                await client.RemoveObjectsAsync(bucket, objectList);
                foreach (var obj in objectList)
                {
                    Assert.False(await client.ObjectExistAsync(bucket, obj));
                }
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                foreach (var obj in objectList)
                {
                    await client.RemoveObjectAsync(bucket, obj);
                }
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task RemovePrefixTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var rootFolder = "test";
            var bucketObjectList = new List<string>();
            for (var i = 0; i < 50; i++)
            {
                var group = i % 2;
                bucketObjectList.Add($"{rootFolder}/{group}/SimpleFile{i}.txt");
            }
            var otherFolder = "test2/1/SimpleFile1.txt";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                // Other folder
                Assert.False(await client.ObjectExistAsync(bucket, otherFolder));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, otherFolder, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, otherFolder));

                // To delete items
                foreach (var item in bucketObjectList)
                {
                    Assert.False(await client.ObjectExistAsync(bucket, item));
                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                    {
                        await client.PutObjectAsync(bucket, item, stream, stream.Length, "text/plain");
                    }
                    Assert.True(await client.ObjectExistAsync(bucket, item));
                }

                var processor = await client.RemovePrefixAsync(bucket, rootFolder, 10);
                processor.WaitComplete();

                foreach (var item in bucketObjectList)
                {
                    Assert.False(await client.ObjectExistAsync(bucket, item), $"Falhou exclusao arquivo: {item}");
                }
                Assert.True(await client.ObjectExistAsync(bucket, otherFolder));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                foreach (var item in bucketObjectList)
                {
                    await client.RemoveObjectAsync(bucket, item);
                }
                await client.RemoveObjectAsync(bucket, otherFolder);
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task RemoveNotExistingPrefixTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var inexistentRootFolderToDelete = "test-invalid";
            var rootFolder = "test";
            var bucketObjectList = new List<string>();
            for (var i = 0; i < 50; i++)
            {
                var group = i % 2;
                bucketObjectList.Add($"{rootFolder}/{group}/SimpleFile{i}.txt");
            }
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "O bucket já existia");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "O bucket não foi criado corretamente");

                foreach (var item in bucketObjectList)
                {
                    Assert.False(await client.ObjectExistAsync(bucket, item), "O objeto já existe antes do upload");
                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                    {
                        await client.PutObjectAsync(bucket, item, stream, stream.Length, "text/plain");
                    }
                    Assert.True(await client.ObjectExistAsync(bucket, item), "O objeto não existe apos upload");
                }

                // Call delete an inexistent Folder, and can´t thrown exception.
                var processor = await client.RemovePrefixAsync(bucket, inexistentRootFolderToDelete, 10);
                processor.WaitComplete();

                foreach (var item in bucketObjectList)
                {
                    Assert.True(await client.ObjectExistAsync(bucket, item), $"Falhou encontrar arquivo: {item}");
                }
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                foreach (var item in bucketObjectList)
                {
                    await client.RemoveObjectAsync(bucket, item);
                }
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task MustRemoveNotExistingPrefixTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var inexistentRootFolderToDelete = "test-invalid";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "O bucket já existia");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "O bucket não foi criado corretamente");

                // Call delete an inexistent Folder, and can´t thrown exception.
                var processor = await client.RemovePrefixAsync(bucket, inexistentRootFolderToDelete, 10);
                processor.WaitComplete();
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task ListObjectsWithoutPrefixTest()
        {
            var client = new MinioStorage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var rootFolder = "test";
            var bucketObjectList = new List<string>();
            for (var i = 0; i < 50; i++)
            {
                var group = i % 2;
                bucketObjectList.Add($"{rootFolder}/{group}/SimpleFile{i}.txt");
            }
            var otherFolder = "test2/1/SimpleFile1.txt";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                // Other folder
                Assert.False(await client.ObjectExistAsync(bucket, otherFolder));
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await client.PutObjectAsync(bucket, otherFolder, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, otherFolder));

                // To delete items
                foreach (var item in bucketObjectList)
                {
                    Assert.False(await client.ObjectExistAsync(bucket, item));
                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                    {
                        await client.PutObjectAsync(bucket, item, stream, stream.Length, "text/plain");
                    }
                    Assert.True(await client.ObjectExistAsync(bucket, item));
                }

                // List items
                var items = await client.ListObjectsAsync(bucket);
                var expectedTotal = bucketObjectList.Count() + 1;
                Assert.Equal(expectedTotal, items.Count());
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                foreach (var item in bucketObjectList)
                {
                    await client.RemoveObjectAsync(bucket, item);
                }
                await client.RemoveObjectAsync(bucket, otherFolder);
                await client.RemoveBucketAsync(bucket);
            }
        }

        [Fact]
        public async Task PutObjectMustHaveHashInMetadata()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-getobject";
            var filePath = "resources/SimpleResourceToAttach.txt";
            var md5Hash = new CreateMD5CheckSum(filePath).GetMd5();
            try
            {
                client.Connect();
                if (!await client.BucketExistsAsync(bucket))
                {
                    await client.MakeBucketAsync(bucket);
                }
                if (await client.ObjectExistAsync(bucket, bucketObject))
                {
                    await client.RemoveObjectAsync(bucket, bucketObject);
                }
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes(filePath)))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));

                var result = await client.GetObjectMetadataAsync(bucket, bucketObject);
                Assert.Equal(md5Hash, result["contentmd5"]);

                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                if (await client.BucketExistsAsync(bucket) && await client.ObjectExistAsync(bucket, bucketObject))
                {
                    await client.RemoveObjectAsync(bucket, bucketObject);
                }
                if (await client.BucketExistsAsync(bucket))
                {
                    await client.RemoveBucketAsync(bucket);
                }
            }
        }

        [Fact]
        public async Task MultipartUploadAsync()
        {
            var client = new MinioStorage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-putobject";
            try
            {
                client.Connect();

                await client.MakeBucketAsync(bucket);

                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    var result = await client.MultipartUploadAsync(bucket, bucketObject, stream, "text/plain");

                    Assert.True(result.SuccessResult);
                }

                Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                if (await client.ObjectExistAsync(bucket, bucketObject))
                {
                    await client.RemoveObjectAsync(bucket, bucketObject);
                }
                if (await client.BucketExistsAsync(bucket))
                {
                    await client.RemoveBucketAsync(bucket);
                }
            }
        }
    }
}
