using System;
using System.Collections.Generic;
using Amazon.S3.Model;
using enki.storage.Interface;
using Minio.DataModel;

namespace enki.storage.Model
{
    public class ObjectInfo : IObjectInfo
    {
        public string ObjectName { get; set; }
        public long Size { get; set; }
        public DateTime LastModified { get; set; }
        public DateTime Expires { get; set; }
        public string ETag { get; set; }
        public string ContentType { get; set; }
        public IDictionary<string, string> MetaData { get; set; }


        public ObjectInfo() { }

        public ObjectInfo(ObjectStat stat)
        {
            ObjectName = stat.ObjectName;
            Size = stat.Size;
            LastModified = stat.LastModified;
            ETag = stat.ETag;
            ContentType = stat.ContentType;
            MetaData = stat.MetaData;

            // Not existent values.
            Expires = DateTime.MaxValue;
        }

        public ObjectInfo(string objectName, GetObjectMetadataResponse stat)
        {
            ObjectName = objectName;
            Size = stat.ContentLength;
            LastModified = stat.LastModified;
            ETag = stat.ETag;
            MetaData = stat.ResponseMetadata.Metadata;
            Expires = stat.Expires == DateTime.MinValue ? DateTime.MaxValue : stat.Expires;
            ContentType = stat.Headers.ContentType;
        }
    }
}