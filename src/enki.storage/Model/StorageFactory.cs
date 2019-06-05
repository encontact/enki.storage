using enki.storage.Interface;

namespace enki.storage.Model
{
    public class StorageFactory
    {
        private IStorageServerConfig _config { get; set; }

        public StorageFactory(IStorageServerConfig config) => _config = config;

        public IStorage Get()
        {
            if (AwsS3Storage.IsAmazonS3Config(_config))
            {
                return new AwsS3Storage(_config);
            }
            else
            {
                return new MinioStorage(_config);
            }
        }
    }
}
