using Amazon.S3.Model;
using enki.storage.Interface;
using Minio.DataModel;
using System;
using System.Collections.Generic;
using System.Globalization;

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

        public ObjectInfo(Item item)
        {
            ObjectName = item.Key;
            Size = (long)item.Size;
            LastModified = item.LastModifiedDateTime.Value;
            ETag = item.ETag;
            ContentType = "application/octet-stream";
            MetaData = new Dictionary<string, string>();

            // Not existent values.
            Expires = DateTime.MaxValue;
        }

        public ObjectInfo(string objectName, GetObjectMetadataResponse stat)
        {
            ObjectName = objectName;
            Size = stat.ContentLength;
            LastModified = stat.LastModified ?? DateTime.MinValue;
            ETag = stat.ETag;
            MetaData = stat.ResponseMetadata.Metadata;

            if (!string.IsNullOrEmpty(stat.ExpiresString) &&
                DateTime.TryParse(
                    stat.ExpiresString,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var expiresParsed))
            {
                Expires = expiresParsed;
            }
            else
            {
                Expires = DateTime.MaxValue;
            }

            ContentType = stat.Headers.ContentType;
        }

        public ObjectInfo(S3Object item)
        {
            ObjectName = item.Key;
            Size = item.Size ?? 0 ;
            LastModified = item.LastModified ?? DateTime.MinValue;
            ETag = item.ETag;
            MetaData = new Dictionary<string, string>();
            ContentType = "application/octet-stream";

            // Not existent values.
            Expires = DateTime.MaxValue;
        }
    }
}