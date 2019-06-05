//using encontact.storage.lib.Interface;
//using EnContactObjectStorageLib.Model;
//using System;
//using System.IO;
//using System.Net;
//using System.Threading.Tasks;
//using Xunit;

//namespace enki.storage.integration.test.TesteStorage
//{
//    public class AwsS3StorageTest
//    {
//        private IStorageServerConfig _config { get; set; }
//        public AwsS3StorageTest() => _config = StorageTestConfig.GetS3Config();

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
//                if (cors == null) Assert.True(false, "A configuração cors inserida não foi encontrada no servidor.");
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
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "O bucket do teste já existe.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "O bucket não existe após ter sido criado.");
//                await client.RemoveBucketAsync(bucket).ConfigureAwait(false);
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "O bucket persiste após remoção");
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
//        public async Task GetObjectTest()
//        {
//            var client = new AwsS3Storage(_config);
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-getobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "Já existe o bucket.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "O bucket não foi criado.");
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "O objeto já existe no servidor");
//                long filePutSize = 0;
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    filePutSize = stream.Length;
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "O objeto não foi criado no servidor");
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
//            var bucketObject = "test/SimpleFile.txt";
//            var bucket = _config.DefaultBucket + "-presign-getobject";
//            try
//            {
//                client.Connect();
//                Assert.False(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "Existe o bucket.");
//                await client.MakeBucketAsync(bucket).ConfigureAwait(false);
//                Assert.True(await client.BucketExistsAsync(bucket).ConfigureAwait(false), "Não existe o bucket.");
//                Assert.False(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "Existe o objecto.");
//                long filePutSize = 0;
//                using (var stream = new MemoryStream(File.ReadAllBytes("resources/SimpleResourceToAttach.txt")))
//                {
//                    filePutSize = stream.Length;
//                    await client.PutObjectAsync(bucket, bucketObject, stream, stream.Length, "text/plain");
//                }
//                Assert.True(await client.ObjectExistAsync(bucket, bucketObject).ConfigureAwait(false), "Não existe o objeto.");
//                var urlToGetObject = await client.PresignedGetObjectAsync(bucket, bucketObject, 60).ConfigureAwait(false);
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
//    }
//}
