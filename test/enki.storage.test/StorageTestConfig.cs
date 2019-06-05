using enki.storage.Interface;

namespace enki.storage.integration.test.TesteStorage
{
    public static class StorageTestConfig
    {
        public static IStorageServerConfig GetMinioConfig() => new StorageConfigTest
        {
            EndPoint = "localhost:9000",
            AccessKey = "dev",
            SecretKey = "secret-dev-test",
            Secure = false,
            DefaultBucket = "enki.storage.test-minio-us-east-1",
            Region = "us-east-1"
        };

        public static IStorageServerConfig GetS3Config() => new StorageConfigTest
        {
            EndPoint = "s3.amazonaws.com",
            AccessKey = "KEY",
            SecretKey = "SECRET",
            Secure = false,
            DefaultBucket = "enki.storage.test-s3-us-east-1",
            Region = "us-east-1"
        };
    }

    public class StorageConfigTest : IStorageServerConfig
    {
        public string EndPoint { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string Region { get; set; }
        public bool Secure { get; set; }
        public string DefaultBucket { get; set; }
    }
}
