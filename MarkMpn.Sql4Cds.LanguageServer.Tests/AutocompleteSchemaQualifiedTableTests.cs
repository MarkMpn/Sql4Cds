using System;
using System.Collections.Generic;
using System.Linq;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk.Metadata;
using AutocompleteEngine = MarkMpn.Sql4Cds.Engine.Autocomplete;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class AutocompleteSchemaQualifiedTableTests
    {
        [TestMethod]
        public void SchemaQualifiedTablesPreloadMetadataUsingLogicalName()
        {
            var entity = new EntityMetadata { LogicalName = "account" };
            var name = new StringAttributeMetadata { LogicalName = "name" };
            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.EntityLogicalName)).SetValue(name, "account");
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity, new AttributeMetadata[] { name });
            var cache = new RecordingMetadataCache(entity);
            var dataSources = new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase)
            {
                ["local"] = new DataSource { Name = "local", Metadata = new MetaMetadataCache(cache) }
            };
            var autocomplete = new AutocompleteEngine(dataSources, "local", ColumnOrdering.Strict);
            var sql = "SELECT name FROM dbo.account";

            autocomplete.GetSuggestions(sql, "SELECT name".Length).ToList();

            CollectionAssert.DoesNotContain(cache.Lookups, "dbo.account");
            CollectionAssert.Contains(cache.Lookups, "account");
        }

        private sealed class RecordingMetadataCache : IAttributeMetadataCache
        {
            private readonly EntityMetadata[] _entities;

            public RecordingMetadataCache(params EntityMetadata[] entities) => _entities = entities;

            public List<string> Lookups { get; } = new List<string>();

            public EntityMetadata this[string name]
            {
                get
                {
                    if (TryGetValue(name, out var metadata))
                        return metadata;

                    throw new KeyNotFoundException(name);
                }
            }

            public EntityMetadata this[int otc] => throw new NotSupportedException();
            public string[] RecycleBinEntities => Array.Empty<string>();
            public IEnumerable<EntityMetadata> GetAllEntities() => _entities;
            public string[] TryGetRecycleBinEntities() => Array.Empty<string>();

            public bool TryGetValue(string logicalName, out EntityMetadata metadata)
            {
                Lookups.Add(logicalName);
                metadata = _entities.SingleOrDefault(entity => entity.LogicalName.Equals(logicalName, StringComparison.OrdinalIgnoreCase));
                return metadata != null;
            }

            public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata) => TryGetValue(logicalName, out metadata);
        }
    }
}
