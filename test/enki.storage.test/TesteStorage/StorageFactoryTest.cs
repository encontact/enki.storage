using enki.storage.Model;
using Xunit;

namespace enki.storage.integration.test.TesteStorage
{
    public class StorageFactoryTests
    {
        private static StorageConfigTest CreateConfigFromEndpoint(string endpoint) => new()
        {
            EndPoint = endpoint,
            AccessKey = "Key",
            SecretKey = "Secret",
            Secure = false,
            DefaultBucket = "enki.storage.test-s3-us-east-1",
            Region = "us-east-1",
        };

        [Theory]
        [InlineData("https://s3.amazonaws.com")]
        [InlineData("https://S3.AMAZONAWS.COM")]
        [InlineData(" https://s3.amazonaws.com ")]
        [InlineData("s3.amazonaws.com")]
        public void Get_ShouldReturnAwsS3Storage_WhenEndpointIsAmazon(string endpoint)
        {
            // Arrange
            var config = CreateConfigFromEndpoint(endpoint);
            var factory = new StorageFactory(config);

            // Act
            var storage = factory.Get();

            // Assert
            Assert.IsType<AwsS3Storage>(storage);
        }

        [Theory]
        [InlineData("http://localhost:4566")]
        [InlineData("http://127.0.0.1:4566")]
        public void Get_ShouldReturnAwsS3Storage_WhenEndpointIsLocalStack(string endpoint)
        {
            // Arrange
            var config = CreateConfigFromEndpoint(endpoint);
            var factory = new StorageFactory(config);

            // Act
            var storage = factory.Get();

            // Assert
            Assert.IsType<AwsS3Storage>(storage);
        }

        [Theory]
        [InlineData("https://minio.local")]
        [InlineData("http://localhost:9000")]
        [InlineData("https://amazon.com")]
        public void Get_ShouldReturnMinioStorage_WhenEndpointIsNotAmazon(string endpoint)
        {
            // Arrange
            var config = CreateConfigFromEndpoint(endpoint);
            var factory = new StorageFactory(config);

            // Act
            var storage = factory.Get();

            // Assert
            Assert.IsType<MinioStorage>(storage);
        }

        [Fact]
        public void Get_ShouldNotThrow_WhenEndpointIsValidMinio()
        {
            // Arrange
            var config = CreateConfigFromEndpoint("http://localhost:9000");
            var factory = new StorageFactory(config);

            // Act & Assert
            var exception = Record.Exception(() => factory.Get());
            Assert.Null(exception);
        }

        [Fact]
        public void Get_ShouldNotThrow_WhenEndpointIsValidAws()
        {
            // Arrange
            var config = CreateConfigFromEndpoint("https://s3.amazonaws.com");
            var factory = new StorageFactory(config);

            // Act & Assert
            var exception = Record.Exception(() => factory.Get());
            Assert.Null(exception);
        }
    }
}