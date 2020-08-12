using enki.storage.Interface;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System;

namespace enki.storage.integration.test.TesteStorage
{
    public enum StorageType
    {
        Minio,
        S3
    }

    public static class StorageTestConfig
    {
        public static IStorageServerConfig GetAppsettingsConfig(StorageType type, string forceRegion = null)
        {
            var config = new ConfigurationBuilder()
                 .AddJsonFile($"config/appsettings.json", optional: true, reloadOnChange: true)
                 .AddJsonFile($"config/appsettings.{EnvironmentName.Development}.json", optional: true, reloadOnChange: true)
                 .Build();

            if (type == StorageType.S3)
            {
                return new StorageConfigTest
                {
                    EndPoint = config["S3:EndPoint"],
                    AccessKey = config["S3:AccessKey"],
                    SecretKey = config["S3:SecretKey"],
                    Secure = Convert.ToBoolean(config["S3:Secure"]),
                    DefaultBucket = config["S3:DefaultBucket"],
                    Region = forceRegion ?? config["S3:Region"]
                };
            }

            return new StorageConfigTest
            {
                EndPoint = config["Minio:EndPoint"],
                AccessKey = config["Minio:AccessKey"],
                SecretKey = config["Minio:SecretKey"],
                Secure = Convert.ToBoolean(config["Minio:Secure"]),
                DefaultBucket = config["Minio:DefaultBucket"],
                Region = forceRegion ?? config["Minio:Region"]
            };
        }

        public static IStorageServerConfig GetAppsettingsConfig(StorageType type)
            => GetAppsettingsConfig(type, null);
    }

    public class StorageConfigTest : IStorageServerConfig
    {
        public string EndPoint { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string Region { get; set; }
        public bool Secure { get; set; }
        public string DefaultBucket { get; set; }

        public bool MustConnectToRegion() => !string.IsNullOrWhiteSpace(Region);
    }
}
