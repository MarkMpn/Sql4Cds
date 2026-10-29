using System;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json;
using StreamJsonRpc;

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
                MetadataCacheContractResolver.Instance.PreloadContracts(typeof(LazyMetadataEntry<EntityMetadata[]>));
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
    }
}
