using enki.storage.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace enki.storage.Interface
{
    public interface IStorage
    {
        IStorageServerConfig ServerConfig { get; }

        void Connect();

        /// <summary>
        /// Indica se a exceção representa "objeto não encontrado" no provider concreto.
        /// Existe para que o consumidor possa tratar ausência sem conhecer as exceções
        /// específicas de S3 ou Minio.
        /// </summary>
        /// <param name="exception">Exceção lançada por uma operação de leitura.</param>
        /// <returns>True se for ausência do objeto, False para qualquer outra falha.</returns>
        bool IsObjectNotFound(Exception exception);
        Task<bool> BucketExistsAsync(string bucketName);
        Task MakeBucketAsync(string bucketName);
        Task MakeBucketAsync(string bucketName, string region);
        Task RemoveBucketAsync(string bucketName);
        Task<PutObjectResponse> PutObjectAsync(string bucketName, string objectName, string filePath, string contentType);
        Task<PutObjectResponse> PutObjectAsync(string bucketName, string objectName, Stream data, long size, string contentType);
        Task<string> PresignedPutObjectAsync(string bucketName, string objectName, int expiresInt, string contentMD5 = null);
        Task RemoveObjectAsync(string bucketName, string objectName);
        Task RemoveObjectsAsync(string bucketName, IEnumerable<string> objects);
        Task<BatchDeleteProcessor> RemovePrefixAsync(string bucketName, string prefix, int chunkSize, CancellationToken cancellationToken = default);
        Task<bool> ObjectExistAsync(string bucketName, string objectName);
        Task<IEnumerable<IObjectInfo>> ListObjectsAsync(string bucketName, string prefix = null);
        Task GetObjectAsync(string bucketName, string objectName, Action<Stream> action);
        Task CopyObjectAsync(string bucketName, string objectName, string destBucketName, string destObjectName);
        Task<IObjectInfo> GetObjectInfoAsync(string bucketName, string objectName);
        Task<IDictionary<string, string>> GetObjectMetadataAsync(string bucketName, string objectName);
        Task<string> PresignedGetObjectAsync(string bucketName, string objectName, int expiresInt, Dictionary<string, string> reqParams = null);
        Task SetCorsToBucketAsync(string bucketName, string allowedOrigin);
        Task<PutObjectResponse> MultipartUploadAsync(
            string bucketName,
            string objectName,
            Stream data,
            string contentType,
            int partSize = 5 * 1024 * 1024,
            CancellationToken cancellationToken = default
        );
    }
}
