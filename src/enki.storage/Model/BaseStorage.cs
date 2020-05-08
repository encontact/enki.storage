using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using enki.storage.Interface;

namespace enki.storage.Model
{
    public class BaseStorage : IStorage
    {
        public IStorageServerConfig ServerConfig { get; protected set; }

        protected BaseStorage(IStorageServerConfig config) => ServerConfig = config;

        public virtual Task<bool> BucketExistsAsync(string bucketName) => throw new NotImplementedException();
        public virtual void Connect() => throw new NotImplementedException();
        public virtual Task GetObjectAsync(string bucketName, string objectName, Action<Stream> action) => throw new NotImplementedException();
        public virtual Task<IObjectInfo> GetObjectInfoAsync(string bucketName, string objectName) => throw new NotImplementedException();
        public virtual Task MakeBucketAsync(string bucketName) => throw new NotImplementedException();
        public virtual Task<bool> ObjectExistAsync(string bucketName, string objectName) => throw new NotImplementedException();
        public virtual Task<string> PresignedPutObjectAsync(string bucketName, string objectName, int expiresInt) => throw new NotImplementedException();
        public virtual Task PutObjectAsync(string bucketName, string objectName, string filePath, string contentType) => throw new NotImplementedException();
        public virtual Task PutObjectAsync(string bucketName, string objectName, Stream data, long size, string contentType) => throw new NotImplementedException();
        public virtual Task RemoveBucketAsync(string bucketName) => throw new NotImplementedException();
        public virtual Task RemoveObjectAsync(string bucketName, string objectName) => throw new NotImplementedException();
        public virtual Task<BatchDeleteProcessor> RemovePrefixAsync(string bucketName, string prefix, int chunkSize, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public virtual Task CopyObjectAsync(string bucketName, string objectName, string destBucketName, string destObjectName) => throw new NotImplementedException();
        public virtual Task<string> PresignedGetObjectAsync(string bucketName, string objectName, int expiresInt, Dictionary<string, string> reqParams = null) => throw new NotImplementedException();
        public virtual Task SetCorsToBucketAsync(string bucketName, string allowedOrigin) => throw new NotImplementedException();

        /// <summary>
        /// Valida se o bucket tem um nome válido para ser utilizado.
        /// 
        /// Bucket names should not contain upper-case letters
        /// Bucket names should not contain underscores(_)
        /// Bucket names should not end with a dash
        /// Bucket names should be between 3 and 63 characters long
        /// Bucket names cannot contain dashes next to periods(e.g., my-.bucket.com and my.-bucket are invalid)
        /// Bucket names cannot contain periods - Due to our S3 client utilizing SSL/HTTPS, Amazon documentation indicates that a bucket name cannot contain a period, otherwise you will not be able to upload files from our S3 browser in the dashboard.
        /// </summary>
        /// <param name="bucketName">Nome do bucket a ser verificado.</param>
        /// <returns>True se é valido e False se não é</returns>
        public static bool IsValidBucketName(string bucketName)
        {
            var pattern = "^[a-z0-9-]{2,63}[a-z0-9]$";
            var regex = new Regex(pattern, RegexOptions.Singleline);
            return regex.IsMatch(bucketName);
        }

        /// <summary>
        /// Valida se o nome do objeto é valido para ser utilizado.
        /// Segue as regras do AWS S3 (http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-keys)
        /// </summary>
        /// <param name="objectName">Nome do objeto para validação.</param>
        /// <returns>True se é valido e False se não é.</returns>
        public static bool IsValidObjectName(string objectName)
        {
            var pattern = @"^[0-9a-zA-Z!&$=;:+,\?\-_.*'@/]{2,500}$";
            var regex = new Regex(pattern, RegexOptions.Singleline);
            return regex.IsMatch(objectName);
        }

    }
}
