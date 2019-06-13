using System;
using System.Collections.Generic;

namespace enki.storage.Interface
{
    public interface IObjectInfo
    {
        string ObjectName { get; }
        long Size { get; }
        DateTime LastModified { get; }
        DateTime Expires { get; }
        string ETag { get; }
        string ContentType { get; }
        IDictionary<string, string> MetaData { get; }
    }
}