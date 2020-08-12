namespace enki.storage.Interface
{
    public interface IStorageServerConfig
    {
        string EndPoint { get; }
        string AccessKey { get; }
        string SecretKey { get; }
        string Region { get; }
        bool Secure { get; }
        string DefaultBucket { get; }

        /// <summary>
        /// If True, the connection will be created setting Region when
        /// construct Client connection.
        /// If False, connection not set the Region, allow Cross-Region operations.
        /// </summary>
        /// <returns>True if must connect region, False if not.</returns>
        bool MustConnectToRegion();
    }
}
