using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.ServiceModel;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Newtonsoft.Json;
using StreamJsonRpc;
using AuthenticationType = Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    class PersistentMetadataCache
    {
        private readonly string _path;
        private readonly JsonRpc _lsp;

        public PersistentMetadataCache(string path, JsonRpc lsp)
        {
            _path = path;
            _lsp = lsp;

            // Generate the contracts used by JSON.NET to serialize the metadata cache in the background.
            // This saves about 0.5 seconds on the first connection.
            _ = Task.Run(() =>
            {
                MetadataCacheContractResolver.Instance.PreloadContracts(typeof(MetadataCache));
            });
        }

        public EntityMetadata GetEntityMetadata(Guid orgId, string version, string key, Func<EntityMetadata> load)
        {
            // Object type code lookups now include attributes and relationships.
            return GetLazyMetadata(orgId, version, key, 2, load);
        }

        public T GetLazyMetadata<T>(Guid orgId, string version, string key, int queryVersion, Func<T> load, bool allowNull = false)
            where T : class
        {
            if (TryGetLazyMetadata(orgId, () => version, key, queryVersion, out T cached, allowNull))
                return cached;

            var entity = load();
            var filename = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))) + ".json.gz";
            var cachePath = Path.Combine(_path, orgId + ".lazy", filename);
            var serializer = new JsonSerializer
            {
                ContractResolver = MetadataCacheContractResolver.Instance,
                TypeNameHandling = TypeNameHandling.Auto
            };

            var directory = Path.GetDirectoryName(cachePath);
            var tempPath = Path.Combine(directory, Guid.NewGuid().ToString("N") + ".tmp");
            try
            {
                Directory.CreateDirectory(directory);
                using (var stream = File.Create(tempPath))
                using (var gz = new GZipStream(stream, CompressionLevel.Optimal))
                using (var writer = new StreamWriter(gz))
                using (var jsonWriter = new JsonTextWriter(writer))
                {
                    serializer.Serialize(jsonWriter, new LazyMetadataEntry<T> { Version = version, QueryVersion = queryVersion, Entity = entity });
                }

                File.Move(tempPath, cachePath, true);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException || ex is JsonException)
            {
                ReportLazyLoadFailure(key, ex);
            }
            finally
            {
                try
                {
                    if (File.Exists(tempPath))
                        File.Delete(tempPath);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
                {
                    ReportLazyLoadFailure(key, ex);
                }
            }

            return entity;
        }

        public bool TryGetLazyMetadata<T>(Guid orgId, Func<string> version, string key, int queryVersion, out T value, bool allowNull = false)
            where T : class
        {
            value = null;
            var filename = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))) + ".json.gz";
            var cachePath = Path.Combine(_path, orgId + ".lazy", filename);
            if (!File.Exists(cachePath))
                return false;

            // Only resolve the stamp when there is a disk entry to validate.
            var currentVersion = version();
            var serializer = new JsonSerializer
            {
                ContractResolver = MetadataCacheContractResolver.Instance,
                TypeNameHandling = TypeNameHandling.Auto
            };
            try
            {
                using (var stream = File.OpenRead(cachePath))
                using (var gz = new GZipStream(stream, CompressionMode.Decompress))
                using (var reader = new StreamReader(gz))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var cached = serializer.Deserialize<LazyMetadataEntry<T>>(jsonReader);
                    if (GetMetadataVersion(cached?.Version) == GetMetadataVersion(currentVersion) &&
                        cached?.QueryVersion == queryVersion && (cached.Entity != null || allowNull))
                    {
                        value = cached.Entity;
                        return true;
                    }
                }
            }
            catch (Exception ex) when (ex is IOException || ex is InvalidDataException || ex is UnauthorizedAccessException || ex is JsonException)
            {
                ReportLazyLoadFailure(key, ex);
            }
            return false;
        }

        private static string GetMetadataVersion(string version)
        {
            // The suffix records when the stamp was retrieved, not a metadata change.
            var separator = version?.IndexOf('!') ?? -1;
            return separator < 0 ? version : version.Substring(0, separator);
        }

        public void ReportLazyLoadFailure(string key, Exception error)
        {
            _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
            {
                MessageType = MessageType.Warning,
                Message = $"Metadata cache for {key}: {error.Message}"
            });
        }

        private class LazyMetadataEntry<T>
        {
            public string Version { get; set; }
            public int QueryVersion { get; set; }
            public T Entity { get; set; }
        }

        public async Task<EntityMetadata[]> GetMetadataAsync(IOrganizationService org, bool flush)
        {
            var svc = org as ServiceClient;

            Version version;
            Guid orgId;

            if (svc != null)
            {
                version = svc.ConnectedOrgVersion;
                orgId = svc.ConnectedOrgId;
            }
            else
            {
                version = new Version(((RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest())).Version);
                orgId = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).OrganizationId;
            }

            if (version.Major < 8)
                return null;

            await _lsp.NotifyAsync("sql4cds/progress", "Metadata Cache Loading");

            var metadataCachePath = Path.Combine(_path, orgId + ".json.gz");

            // Set up the serializer to use. We need to add a custom converter to handle the KeyAttributeCollection on
            // the Entity class (used for the AsyncJob property on EntityKeyMetadata) as the standard JsonSerializer
            // can't handle it. We also need to set the TypeNameHandling property to Auto to ensure polymorphic classes
            // (e.g. attribute types) are serialized correctly.
            var metadataSerializer = new JsonSerializer();
            metadataSerializer.ContractResolver = MetadataCacheContractResolver.Instance;
            metadataSerializer.TypeNameHandling = TypeNameHandling.Auto;

            MetadataCache metadataCache = null;

            // Load the existing file if it exists and we're not trying to just update an already-loaded cache.
            if (File.Exists(metadataCachePath) && !flush)
            {
                try
                {
                    using (var stream = File.OpenRead(metadataCachePath))
                    using (var gz = new GZipStream(stream, CompressionMode.Decompress))
                    using (var reader = new StreamReader(gz))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        metadataCache = metadataSerializer.Deserialize<MetadataCache>(jsonReader);
                    }
                }
                catch
                {
                    // If the cache file isn't readable for any reason, throw it away and download a new copy
                }
            }

            // Get all the metadata that's changed since the last connection
            // If this query changes, increment the version number to ensure any previously cached versions are flushed
            const int queryVersion = 2;

            if (metadataCache != null && metadataCache.MetadataQueryVersion != queryVersion)
            {
                metadataCache = null;
                flush = true;
            }

            var metadataQuery = new RetrieveMetadataChangesRequest
            {
                ClientVersionStamp = !flush ? metadataCache?.ClientVersionStamp : null,
                Query = new EntityQueryExpression
                {
                    Properties = new MetadataPropertiesExpression { AllProperties = true },
                    AttributeQuery = new AttributeQueryExpression
                    {
                        Properties = new MetadataPropertiesExpression { AllProperties = true }
                    },
                    RelationshipQuery = new RelationshipQueryExpression
                    {
                        Properties = new MetadataPropertiesExpression { AllProperties = true }
                    }
                },
                DeletedMetadataFilters = DeletedMetadataFilters.All
            };

            RetrieveMetadataChangesResponse metadataUpdate;

            // Use a cloned connection instance where possible so we can load the metadata in the background without
            // blocking other uses of the connection.
            if (svc?.ActiveAuthenticationType == AuthenticationType.OAuth)
                org = svc.Clone();

            try
            {
                metadataUpdate = (RetrieveMetadataChangesResponse)org.Execute(metadataQuery);
            }
            catch (FaultException<OrganizationServiceFault> ex)
            {
                // If the last connection was too long ago, we need to request all the metadata, not just the changes
                if (ex.Detail.ErrorCode == unchecked((int)0x80044352))
                {
                    metadataQuery.ClientVersionStamp = null;
                    metadataUpdate = (RetrieveMetadataChangesResponse)org.Execute(metadataQuery);
                }
                else
                {
                    await _lsp.NotifyAsync("sql4cds/progress", "Metadata Cache Loading Failed");

                    throw;
                }
            }

            if (metadataQuery.ClientVersionStamp != null && metadataQuery.ClientVersionStamp != metadataUpdate.ServerVersionStamp)
            {
                // Something has changed in the metadata. We can't reliably apply changes as some of the IDs
                // appear to change during backup/restore operations, so get a whole fresh copy
                metadataQuery.ClientVersionStamp = null;
                metadataUpdate = (RetrieveMetadataChangesResponse)org.Execute(metadataQuery);
            }

            if (metadataQuery.ClientVersionStamp == null)
            {
                // Save the latest metadata cache
                metadataCache = new MetadataCache();
                metadataCache.EntityMetadata = metadataUpdate.EntityMetadata.ToArray();
                metadataCache.ClientVersionStamp = metadataUpdate.ServerVersionStamp;
                metadataCache.MetadataQueryVersion = queryVersion;

                _ = Task.Run(() =>
                {
                    // Write the new metadata to a temporary file in the same directory, then swap it with the original
                    // file. This avoids getting a corrupted file if something goes wrong while the file is being written.
                    var directory = Path.GetDirectoryName(metadataCachePath);
                    Directory.CreateDirectory(directory);

                    var tempFileName = "~" + Path.GetFileName(metadataCachePath);
                    var tempFilePath = Path.Combine(directory, tempFileName);

                    using (var stream = File.Create(tempFilePath))
                    using (var gz = new GZipStream(stream, CompressionLevel.Optimal))
                    using (var writer = new StreamWriter(gz))
                    using (var jsonWriter = new JsonTextWriter(writer))
                    {
                        metadataSerializer.Serialize(jsonWriter, metadataCache);
                    }

                    if (File.Exists(metadataCachePath))
                        File.Replace(tempFilePath, metadataCachePath, null);
                    else
                        File.Move(tempFilePath, metadataCachePath);

                    _ = _lsp.NotifyAsync("sql4cds/progress", "Metadata Cache Updated");
                });
            }

            await _lsp.NotifyAsync("sql4cds/progress", "Metadata Cache Loaded");

            return metadataCache.EntityMetadata;
        }
    }
}
