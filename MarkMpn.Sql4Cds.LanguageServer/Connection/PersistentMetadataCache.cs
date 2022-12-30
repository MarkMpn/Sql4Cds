using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.ServiceModel;
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
            Task.Run(() =>
            {
                MetadataCacheContractResolver.Instance.PreloadContracts(typeof(MetadataCache));
            });
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
