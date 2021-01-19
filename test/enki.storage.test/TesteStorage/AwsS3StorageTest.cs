//using enki.storage.Interface;
//using enki.storage.Model;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Net;
//using System.Security.Cryptography;
//using System.Text;
//using System.Threading.Tasks;
//using Xunit;

//namespace enki.storage.integration.test.TesteStorage
//{
//    public class AwsS3StorageTest
//    {
//        private IStorageServerConfig _config { get; set; }
//        private IStorageServerConfig _configWithoutRegion { get; set; }
//        public AwsS3StorageTest()
//        {
//            _config = StorageTestConfig.GetAppsettingsConfig(StorageType.S3);
//            // Força configuração sem região.
//            _configWithoutRegion = StorageTestConfig.GetAppsettingsConfig(StorageType.S3, "");
//        }

//        [Fact]
//        public async Task NotFoundBucketExistsAsyncTest()
//        {
//            var bucket = _config.DefaultBucket + "-notfoundbucket";
//            try
//            {
//                var client = new AwsS3Storage(_config);
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//        }

//        [Fact]
//        public async Task TestCreateBucket()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-createdbucket";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//        [Fact]
//        public async Task SetCorsToCreatedBucketTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-cors";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.SetCorsToBucketAsync(bucket, "*").ConfigureAwait(false);
//                var cors = await client.RetrieveCORSConfigurationAsync(bucket).ConfigureAwait(false);
//                if (cors == null) Assert.True(false, "The CORS config not found on server.");
//                Assert.Single(cors.Rules);
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//        [Fact]
//        public async Task TestDeleteBucket()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-deletebucket";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket already exists.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket was not created with success.");
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket exists after delete.");
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task PutObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-putobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task PresignPutObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-presignputobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60).ConfigureAwait(false);

//                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
//                httpRequest.Method = "PUT";
//                using (var dataStream = httpRequest.GetRequestStream())
//                {
//                    var buffer = new byte[8000];
//                    using (var fileStream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                    {
//                        var bytesRead = 0;
//                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
//                        {
//                            dataStream.Write(buffer, 0, bytesRead);
//                        }
//                    }
//                }
//                var response = httpRequest.GetResponse() as HttpWebResponse;
//                if (response.StatusCode != HttpStatusCode.OK) Assert.True(false, "Error on HTTP result.");
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task PresignPutObjectWithMd5ValidationTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-presignputobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                var fileBytes = File.ReadAllBytes("resources/SimpleResourceToAttach.txt");
//                var fileMD5 = GetMd5(fileBytes);

//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60, fileMD5).ConfigureAwait(false);

//                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
//                httpRequest.Method = "PUT";
//                httpRequest.Headers.Add("Content-MD5", fileMD5);

//                using (var dataStream = httpRequest.GetRequestStream())
//                {
//                    var buffer = new byte[8000];
//                    using (var fileStream = new MemoryStream(fileBytes))
//                    {
//                        var bytesRead = 0;
//                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
//                        {
//                            dataStream.Write(buffer, 0, bytesRead);
//                        }
//                    }
//                }

//                var response = httpRequest.GetResponse() as HttpWebResponse;
//                if (response.StatusCode != HttpStatusCode.OK) Assert.True(false, "Envio HTTP não acusou sucesso.");
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task ErrorOnPresignPutObjectWithInvalidFileTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-presignputobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                var fileBytes = File.ReadAllBytes("resources/SimpleResourceToAttach.txt");
//                var fileMD5 = GetMd5(fileBytes);

//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                var urlToUpload = await client.PresignedPutObjectAsync(bucket, bucketObject, 60, fileMD5).ConfigureAwait(false);

//                var httpRequest = WebRequest.Create(urlToUpload) as HttpWebRequest;
//                httpRequest.Method = "PUT";
//                httpRequest.Headers.Add("Content-MD5", fileMD5);

//                using (var dataStream = httpRequest.GetRequestStream())
//                {
//                    var buffer = new byte[8000];
//                    var wrongFile = Encoding.ASCII.GetBytes("xxxxxxxx");
//                    using (var fileStream = new MemoryStream(wrongFile))
//                    {
//                        var bytesRead = 0;
//                        while ((bytesRead = fileStream.Read(buffer, 0, buffer.Length)) > 0)
//                        {
//                            dataStream.Write(buffer, 0, bytesRead);
//                        }
//                    }
//                }

//                try
//                {
//                    var response = httpRequest.GetResponse() as HttpWebResponse;
//                    if (response.StatusCode == HttpStatusCode.OK) Assert.True(false, "Envio HTTP não acusou erro.");
//                }
//                catch (WebException ex)
//                {
//                    Assert.Equal("The remote server returned an error: (400) Bad Request.", ex.Message);
//                }
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        private string GetMd5(byte[] file)
//        {
//            using (var md5 = MD5.Create())
//            {
//                var hash = md5.ComputeHash(file);
//                return Convert.ToBase64String(hash);
//            }
//        }

//        [Fact]
//        public async Task GetObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-getobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket already exists.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket wasn´t created");
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "The object already exists on server");
//                long filePutSize = 0;
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    filePutSize = stream.Length;
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "The object was not created on server");
//                using (var dataStream = new MemoryStream())
//                {
//                    await client.GetObjectAsync(bucket, bucketObject,
//                        (stream) =>
//                        {
//                            stream.CopyTo(dataStream);
//                        }
//                    );
//                    Assert.Equal(filePutSize, dataStream.Length);
//                }
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task PresignGetObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var fileName = "SimpleFile.txt";
//            var bucketObject = $"test/{fileName}";
//            var bucket = _config.DefaultBucket + "-presign-getobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket already exists.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket was´nt exists.");
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "The object already exists");
//                long filePutSize = 0;
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    filePutSize = stream.Length;
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "The object was not exists");
//                var urlToGetObject = await client.PresignedGetObjectAsync(bucket, bucketObject, 60).ConfigureAwait(false);
//                Assert.Contains(fileName, urlToGetObject);
//                using (var clientWeb = new WebClient())
//                {
//                    var data = await clientWeb.DownloadDataTaskAsync(new Uri(urlToGetObject)).ConfigureAwait(false);
//                    Assert.Equal(filePutSize, data.Length);
//                }
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task CopyObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var destBucketObject = "test/SimpleCopiedFile.txt";
//            var bucket = _config.DefaultBucket + "-copy-object";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                await client.CopyObjectAsync(bucket, bucketObject, bucket, destBucketObject).ConfigureAwait(false);
//                Assert.True(await client.ObjectExistAsync(bucket, destBucketObject).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                }
//                if (await client.ObjectExistAsync(bucket, destBucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(bucket, destBucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(bucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task CopyObjectBetweenRegionsTest()
//        {
//            var client = new AwsS3Storage(_configWithoutRegion);

//            var originRegion = "us-east-1";
//            var originBucketObject = "test/SimpleFile.txt";
//            var originBucket = $"virginia-copy-object";

//            var destRegion = "us-west-2";
//            var destBucketObject = "test/SimpleCopiedFile.txt";
//            var destBucket = "oregon-copy-object";

//            try
//            {
//                client.Connect();

//                #region Origin bucket + file upload
//                Assert.False(await client.BucketExistsAsync(originBucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(originBucket, originRegion).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(originBucket).ConfigureAwait(false));
//                Assert.False(await client.ObjectExistAsync(originBucket, destBucketObject).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(originBucket, originBucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(originBucket, originBucketObject).ConfigureAwait(false));
//                #endregion

//                #region Create destiny bucket
//                Assert.False(await client.BucketExistsAsync(destBucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(destBucket, destRegion).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(destBucket).ConfigureAwait(false));
//                Assert.False(await client.ObjectExistAsync(destBucket, destBucketObject).ConfigureAwait(false));
//                #endregion

//                await client.CopyObjectAsync(originBucket, originBucketObject, destBucket, destBucketObject).ConfigureAwait(false);
//                Assert.True(await client.ObjectExistAsync(destBucket, destBucketObject).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                if (await client.ObjectExistAsync(originBucket, originBucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(originBucket, originBucketObject).ConfigureAwait(false);
//                }
//                if (await client.ObjectExistAsync(destBucket, destBucketObject).ConfigureAwait(false))
//                {
//                    await client.RemoveObjectAsync(destBucket, destBucketObject).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(originBucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(originBucket).ConfigureAwait(false);
//                }
//                if (await client.BucketExistsAsync(destBucket).ConfigureAwait(false))
//                {
//                    await client.RemoveBucketAsync(destBucket).ConfigureAwait(false);
//                }
//            }
//        }

//        [Fact]
//        public async Task GetObjectInfoAsyncTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-object-info";
//            try
//            {
//                // Prepare
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));

//                // Test
//                var data = await client.GetObjectInfoAsync(bucket, bucketObject).ConfigureAwait(false);
//                Assert.NotNull(data);
//                Assert.Equal("\"bfece62529a41e4f0c16cd72c81ab8ba\"", data.ETag);
//                Assert.Equal("text/plain", data.ContentType);
//                Assert.Equal(DateTime.MaxValue, data.Expires);
//                Assert.Equal(DateTime.UtcNow.Year, data.LastModified.Year);
//                Assert.Equal(DateTime.UtcNow.Month, data.LastModified.Month);
//                Assert.Equal(DateTime.UtcNow.Day, data.LastModified.Day);
//                Assert.Equal(bucketObject, data.ObjectName);
//                Assert.Equal(54, data.Size);
//                Assert.Equal(1, data.MetaData.Count);
//                Assert.True(data.MetaData.ContainsKey("x-amz-id-2"));

//                // Clean
//                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                await client.RemoveObjectAsync(bucket, bucketObject).ConfigureAwait(false);
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                Assert.False(true, e.Message);
//            }
//        }

//        [Fact]
//        public async Task RemovePrefixTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-removeprefix";
//            var rootFolder = "test";
//            var bucketObjectList = new List<string>();
//            for (var i = 0; i < 50; i++)
//            {
//                var group = i % 2;
//                bucketObjectList.Add($"{rootFolder}/{group}/SimpleFile{i}.txt");
//            }
//            var otherFolder = "test2/1/SimpleFile1.txt";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                // Other folder
//                Assert.False(await client.ObjectExistAsync(bucket, otherFolder).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(bucket, otherFolder, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, otherFolder).ConfigureAwait(false));

//                // To delete items
//                foreach (var item in bucketObjectList)
//                {
//                    Assert.False(await client.ObjectExistAsync(bucket, item).ConfigureAwait(false));
//                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                    {
//                        await client.PutObjectAsync(bucket, item, stream, stream.Length, "text/plain");
//                    }
//                    Assert.True(await client.ObjectExistAsync(bucket, item).ConfigureAwait(false));
//                }

//                var processor = await client.RemovePrefixAsync(bucket, rootFolder, 10).ConfigureAwait(false);
//                processor.WaitComplete();

//                foreach (var item in bucketObjectList)
//                {
//                    Assert.False(await client.ObjectExistAsync(bucket, item).ConfigureAwait(false), $"Fail on delete file: {item}");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, otherFolder).ConfigureAwait(false));
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                foreach (var item in bucketObjectList)
//                {
//                    await client.RemoveObjectAsync(bucket, item).ConfigureAwait(false);
//                }
//                await client.RemoveObjectAsync(bucket, otherFolder).ConfigureAwait(false);
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//        [Fact]
//        public async Task MustRemoveNotExistingPrefixTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-removeprefix";
//            var inexistentRootFolderToDelete = "test-invalid";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket already exists");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "The bucket is not created with success");

//                // Call delete an inexistent Folder, and can´t thrown exception.
//                var processor = await client.RemovePrefixAsync(bucket, inexistentRootFolderToDelete, 10).ConfigureAwait(false);
//                processor.WaitComplete();
//                Assert.True(true, "Process without errors.");
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//        [Fact]
//        public async Task ListObjectsWithoutPrefixTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-removeprefix";
//            var rootFolder = "test";
//            var bucketObjectList = new List<string>();
//            for (var i = 0; i < 22; i++)
//            {
//                var group = i % 2;
//                bucketObjectList.Add($"{rootFolder}/{group}/SimpleFile{i}.txt");
//            }
//            var otherFolder = "test2/1/SimpleFile1.txt";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                // Other folder
//                Assert.False(await client.ObjectExistAsync(bucket, otherFolder).ConfigureAwait(false));
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    await client.PutObjectAsync(bucket, otherFolder, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, otherFolder).ConfigureAwait(false));

//                // To delete items
//                foreach (var item in bucketObjectList)
//                {
//                    Assert.False(await client.ObjectExistAsync(bucket, item).ConfigureAwait(false));
//                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                    {
//                        await client.PutObjectAsync(bucket, item, stream, stream.Length, "text/plain");
//                    }
//                    Assert.True(await client.ObjectExistAsync(bucket, item).ConfigureAwait(false));
//                }

//                // List items
//                var items = await client.ListObjectsAsync(bucket).ConfigureAwait(false);
//                var expectedTotal = bucketObjectList.Count() + 1;
//                Assert.Equal(expectedTotal, items.Count());
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                foreach (var item in bucketObjectList)
//                {
//                    await client.RemoveObjectAsync(bucket, item).ConfigureAwait(false);
//                }
//                await client.RemoveObjectAsync(bucket, otherFolder).ConfigureAwait(false);
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//        [Fact]
//        public async Task RemoveObjectsTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucket = _config.DefaultBucket + "-deleteobjects";
//            var itemQuantity = 10;
//            var objectList = new List<string>();
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false));
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false));

//                for (int i = 0; i < itemQuantity; i++)
//                {
//                    var bucketObject = $"test/SimpleFile{i}.txt";
//                    Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));

//                    using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                    {
//                        await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                    }

//                    Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false));
//                    objectList.Add(bucketObject);
//                }

//                await client.RemoveObjectsAsync(bucket, objectList).ConfigureAwait(false);
//                foreach (var obj in objectList)
//                {
//                    Assert.False(await client.ObjectExistAsync(bucket, obj).ConfigureAwait(false));
//                }
//            }
//            catch (Exception e)
//            {
//                Assert.False(true, e.Message);
//            }
//            finally
//            {
//                foreach (var obj in objectList)
//                {
//                    await client.RemoveObjectAsync(bucket, obj).ConfigureAwait(false);
//                }
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//            }
//        }

//    }
//}
