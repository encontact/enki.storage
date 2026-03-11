using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using enki.storage.Interface;
using enki.storage.Model;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    public class AwsS3StorageTest
    {
        private IStorageServerConfig _config { get; set; }
        private IStorageServerConfig _configWithoutRegion { get; set; }
        public AwsS3StorageTest()
        {
            _config = StorageTestConfig.GetAppsettingsConfig(StorageType.S3);
            // Força configuração sem região.
            _configWithoutRegion = StorageTestConfig.GetAppsettingsConfig(StorageType.S3, "");
        }

        [Fact]
        public async Task NotFoundBucketExistsAsyncTest()
        {
            var bucket = _config.DefaultBucket + "-notfoundbucket";
            try
            {
                var client = new AwsS3Storage(_config);
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
            var client = new AwsS3Storage(_config);
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
            var client = new AwsS3Storage(_config);
            var bucket = _config.DefaultBucket + "-cors";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));
                await client.SetCorsToBucketAsync(bucket, "*");
                var cors = await client.RetrieveCORSConfigurationAsync(bucket);
                if (cors == null) Assert.Fail("The CORS config not found on server.");
                Assert.Single(cors.Rules);
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
            var client = new AwsS3Storage(_config);
            var bucket = _config.DefaultBucket + "-deletebucket";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "The bucket already exists.");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "The bucket was not created with success.");
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket), "The bucket exists after delete.");
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
            finally
            {
                if (await client.BucketExistsAsync(bucket))
                {
                    await client.RemoveBucketAsync(bucket);
                }
            }
        }

        [Fact]
        public async Task PutObjectTest()
        {
            var client = new AwsS3Storage(_config);
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

        [Fact]
        public async Task PresignPutObjectTest()
        {
            var client = new AwsS3Storage(_config);
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
                if (response.StatusCode != HttpStatusCode.OK) Assert.Fail("Error on HTTP result.");
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

        [Fact]
        public async Task PresignPutObjectWithMd5ValidationTest()
        {
            var client = new AwsS3Storage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-presignputobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                var fileBytes = File.ReadAllBytes("resources/SimpleResourceToAttach.txt");
                var fileMD5 = GetMd5(fileBytes);

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60, fileMD5);

                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
                httpRequest.Method = "PUT";
                httpRequest.Headers.Add("Content-MD5", fileMD5);

                using (var dataStream = httpRequest.GetRequestStream())
                {
                    var buffer = new byte[8000];
                    using (var fileStream = new MemoryStream(fileBytes))
                    {
                        var bytesRead = 0;
                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            dataStream.Write(buffer, 0, bytesRead);
                        }
                    }
                }

                var response = httpRequest.GetResponse() as HttpWebResponse;
                if (response.StatusCode != HttpStatusCode.OK) Assert.Fail("Envio HTTP não acusou sucesso.");
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

        [Fact]
        public async Task ErrorOnPresignPutObjectWithInvalidFileTest()
        {
            var client = new AwsS3Storage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-presignputobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                var fileBytes = File.ReadAllBytes("resources/SimpleResourceToAttach.txt");
                var fileMD5 = GetMd5(fileBytes);

                Assert.False(await client.ObjectExistAsync(bucket, bucketObject));
                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60, fileMD5);

                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
                httpRequest.Method = "PUT";
                httpRequest.Headers.Add("Content-MD5", fileMD5);

                using (var dataStream = httpRequest.GetRequestStream())
                {
                    var buffer = new byte[8000];
                    var wrongFile = Encoding.ASCII.GetBytes("xxxxxxxx");
                    using (var fileStream = new MemoryStream(wrongFile))
                    {
                        var bytesRead = 0;
                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            dataStream.Write(buffer, 0, bytesRead);
                        }
                    }
                }

                try
                {
                    var response = httpRequest.GetResponse() as HttpWebResponse;
                    if (response.StatusCode == HttpStatusCode.OK) Assert.Fail("Envio HTTP não acusou erro.");
                }
                catch (WebException ex)
                {
                    Assert.Equal("The remote server returned an error: (400) Bad Request.".ToLowerInvariant(), ex.Message.ToLowerInvariant());
                }
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

        private string GetMd5(byte[] file)
        {
            using (var md5 = MD5.Create())
            {
                var hash = md5.ComputeHash(file);
                return Convert.ToBase64String(hash);
            }
        }

        [Fact]
        public async Task GetObjectTest()
        {
            var client = new AwsS3Storage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-getobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "The bucket already exists.");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "The bucket wasn´t created");
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject), "The object already exists on server");
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject), "The object was not created on server");
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

        [Fact]
        public async Task PresignGetObjectTest()
        {
            var client = new AwsS3Storage(_config);
            var fileName = "SimpleFile.txt";
            var bucketObject = $"test/{fileName}";
            var bucket = _config.DefaultBucket + "-presign-getobject";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "The bucket already exists.");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "The bucket was´nt exists.");
                Assert.False(await client.ObjectExistAsync(bucket, bucketObject), "The object already exists");
                long filePutSize = 0;
                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    filePutSize = stream.Length;
                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                }
                Assert.True(await client.ObjectExistAsync(bucket, bucketObject), "The object was not exists");
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

        [Fact]
        public async Task CopyObjectTest()
        {
            var client = new AwsS3Storage(_config);
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
                if (await client.ObjectExistAsync(bucket, destBucketObject))
                {
                    await client.RemoveObjectAsync(bucket, destBucketObject);
                }
                if (await client.BucketExistsAsync(bucket))
                {
                    await client.RemoveBucketAsync(bucket);
                }
            }
        }

        [Fact]
        public async Task CopyObjectBetweenRegionsTest()
        {
            var originRegion = "us-east-1";
            var originBucketObject = "test/SimpleFile.txt";
            var originBucket = "virginia-copy-object";

            var destRegion = "us-west-2";
            var destBucketObject = "test/SimpleCopiedFile.txt";
            var destBucket = "oregon-copy-object";

            var originConfig = StorageTestConfig.GetAppsettingsConfig(StorageType.S3);
            originConfig.Region = originRegion;
            var destConfig = StorageTestConfig.GetAppsettingsConfig(StorageType.S3);
            destConfig.Region = destRegion;

            var originClient = new AwsS3Storage(originConfig);
            var destClient = new AwsS3Storage(destConfig);

            try
            {
                originClient.Connect();
                destClient.Connect();

                // ORIGIN: bucket + upload
                Assert.False(await originClient.BucketExistsAsync(originBucket));
                await originClient.MakeBucketAsync(originBucket, originRegion);
                Assert.True(await originClient.BucketExistsAsync(originBucket));
                Assert.False(await originClient.ObjectExistAsync(originBucket, destBucketObject));

                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                {
                    await originClient.PutObjectAsync(originBucket, originBucketObject, stream, stream.Length, "text/plain");
                }

                Assert.True(await originClient.ObjectExistAsync(originBucket, originBucketObject));

                // DEST: criar bucket
                Assert.False(await destClient.BucketExistsAsync(destBucket));
                await destClient.MakeBucketAsync(destBucket, destRegion);
                Assert.True(await destClient.BucketExistsAsync(destBucket));
                Assert.False(await destClient.ObjectExistAsync(destBucket, destBucketObject));

                // copy entre regiões usando o client da região de destino
                await destClient.CopyObjectAsync(originBucket, originBucketObject, destBucket, destBucketObject);

                Assert.True(await destClient.ObjectExistAsync(destBucket, destBucketObject));
            }
            finally
            {
                // cleanup origem
                if (await originClient.ObjectExistAsync(originBucket, originBucketObject))
                    await originClient.RemoveObjectAsync(originBucket, originBucketObject);

                if (await originClient.BucketExistsAsync(originBucket))
                    await originClient.RemoveBucketAsync(originBucket);

                // cleanup destino
                if (await destClient.ObjectExistAsync(destBucket, destBucketObject))
                    await destClient.RemoveObjectAsync(destBucket, destBucketObject);

                if (await destClient.BucketExistsAsync(destBucket))
                    await destClient.RemoveBucketAsync(destBucket);
            }
        }

        [Fact]
        public async Task GetObjectInfoAsyncTest()
        {
            var client = new AwsS3Storage(_config);
            var bucketObject = "test/SimpleFile.txt";
            var bucket = _config.DefaultBucket + "-object-info";
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
                Assert.Equal("\"bfece62529a41e4f0c16cd72c81ab8ba\"", data.ETag);
                Assert.Equal("text/plain", data.ContentType);
                Assert.Equal(DateTime.MaxValue, data.Expires);
                Assert.Equal(DateTime.UtcNow.Year, data.LastModified.Year);
                Assert.Equal(DateTime.UtcNow.Month, data.LastModified.Month);
                Assert.Equal(DateTime.UtcNow.Day, data.LastModified.Day);
                Assert.Equal(bucketObject, data.ObjectName);
                Assert.Equal(54, data.Size);
                Assert.NotEmpty(data.MetaData);
                Assert.True(data.MetaData.ContainsKey("x-amz-id-2"));

                // Clean
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.False(await client.BucketExistsAsync(bucket));
            }
            catch (Exception e)
            {
                await client.RemoveObjectAsync(bucket, bucketObject);
                await client.RemoveBucketAsync(bucket);
                Assert.Fail(e.Message);
            }
        }

        [Fact]
        public async Task RemovePrefixTest()
        {
            var client = new AwsS3Storage(_config);
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
                    Assert.False(await client.ObjectExistAsync(bucket, item), $"Fail on delete file: {item}");
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
        public async Task MustRemoveNotExistingPrefixTest()
        {
            var client = new AwsS3Storage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var inexistentRootFolderToDelete = "test-invalid";
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket), "The bucket already exists");
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket), "The bucket is not created with success");

                // Call delete an inexistent Folder, and can´t thrown exception.
                var processor = await client.RemovePrefixAsync(bucket, inexistentRootFolderToDelete, 10);
                processor.WaitComplete();
                Assert.True(true, "Process without errors.");
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
            var client = new AwsS3Storage(_config);
            var bucket = _config.DefaultBucket + "-removeprefix";
            var rootFolder = "test";
            var bucketObjectList = new List<string>();
            for (var i = 0; i < 22; i++)
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
        public async Task RemoveObjectsTest()
        {
            var client = new AwsS3Storage(_config);
            var bucket = _config.DefaultBucket + "-deleteobjects";
            var itemQuantity = 10;
            var objectList = new List<string>();
            try
            {
                client.Connect();
                Assert.False(await client.BucketExistsAsync(bucket));
                await client.MakeBucketAsync(bucket);
                Assert.True(await client.BucketExistsAsync(bucket));

                for (int i = 0; i < itemQuantity; i++)
                {
                    var bucketObject = $"test/SimpleFile{i}.txt";
                    Assert.False(await client.ObjectExistAsync(bucket, bucketObject));

                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
                    {
                        await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
                    }

                    Assert.True(await client.ObjectExistAsync(bucket, bucketObject));
                    objectList.Add(bucketObject);
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
        public async Task PutObjectMustHaveHashInMetadata()
        {
            var client = new AwsS3Storage(_config);
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
            var client = new AwsS3Storage(_config);
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
