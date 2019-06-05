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
    }
}
