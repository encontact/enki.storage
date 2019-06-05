using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace enki.storage.Interface
{
    public interface IStorage
    {
        IStorageServerConfig ServerConfig { get; }

        void Connect();
        Task<bool> BucketExistsAsync(string bucketName);
        Task MakeBucketAsync(string bucketName);
        Task RemoveBucketAsync(string bucketName);
        Task PutObjectAsync(string bucketName, string objectName, string filePath, string contentType);
        Task PutObjectAsync(string bucketName, string objectName, Stream data, long size, string contentType);
        Task<string> PresignedPutObjectAsync(string bucketName, string objectName, int expiresInt);
        Task RemoveObjectAsync(string bucketName, string objectName);
        Task<bool> ObjectExistAsync(string bucketName, string objectName);
        Task GetObjectAsync(string bucketName, string objectName, Action<Stream> action);
        Task CopyObjectAsync(string bucketName, string objectName, string destBucketName, string destObjectName);
        Task<string> PresignedGetObjectAsync(string bucketName, string objectName, int expiresInt, Dictionary<string, string> reqParams = null);
        Task SetCorsToBucketAsync(string bucketName, string allowedOrigin);
    }
}
