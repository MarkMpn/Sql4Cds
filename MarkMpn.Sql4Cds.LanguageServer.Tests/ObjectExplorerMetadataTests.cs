using System;
using System.Collections.Generic;
using System.Linq;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class ObjectExplorerMetadataTests
    {
        [DataTestMethod]
        [DataRow("https://contoso.crm.dynamics.com", true)]
        [DataRow("https://CONTOSO.CRM.DYNAMICS.COM/org/", true)]
        [DataRow("https://crm.example.test/org", false)]
        [DataRow("https://notdynamics.com", false)]
        [DataRow("https://contoso.crm.dynamics.com.example.test", false)]
        public void OnlineDetectionUsesCustomServiceClientUri(string url, bool expected)
        {
            var service = new PptbOrganizationService(new MetadataTestHelper.MetadataTransport(Array.Empty<Newtonsoft.Json.Linq.JObject>()), url, "test");
            var client = (IServiceClient)service;
            Assert.AreEqual(new Uri(url), client.Uri);
            Assert.AreEqual(client.Uri, ((IServiceClient)client.Clone()).Uri);
            Assert.AreEqual(expected, ObjectExplorerHandler.IsOnline(service));
        }

        [TestMethod]
        public void RecordSelectQueryQualifiesCrossInstanceReferencesAndEscapesIdentifiers()
        {
            var id = new Guid("00000000-0000-0000-0000-000000000123");
            var entity = new EntityMetadata { LogicalName = "email" };
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryIdAttribute)).SetValue(entity, "activityid");

            Assert.AreEqual($"SELECT * FROM email WHERE activityid = '{id}'",
                ScriptGenerator.GenerateRecordSelectQuery("Other Source", "OTHER SOURCE", entity, id));
            Assert.AreEqual($"SELECT * FROM [Other Source].dbo.email WHERE activityid = '{id}'",
                ScriptGenerator.GenerateRecordSelectQuery("First", "Other Source", entity, id));
            Assert.AreEqual($"SELECT * FROM [Other Source].dbo.email WHERE activityid = '{id}'",
                ScriptGenerator.GenerateRecordSelectQuery(null, "Other Source", entity, id));

            entity.LogicalName = "table]name";
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryIdAttribute)).SetValue(entity, "key]name");
            Assert.AreEqual($"SELECT * FROM [table]]name] WHERE [key]]name] = '{id}'",
                ScriptGenerator.GenerateRecordSelectQuery("First", "First", entity, id));
        }

        [TestMethod]
        public void TablesUseCachedEntityListAndArchiveFlagsWithoutLoadingFullMetadata()
        {
            var cache = new MetadataCache(
                new EntityMetadata { LogicalName = "zeta", IsArchivalEnabled = false, IsRetentionEnabled = false },
                new EntityMetadata { LogicalName = "retained", IsRetentionEnabled = true },
                new EntityMetadata { LogicalName = "archived", IsArchivalEnabled = true },
                new EntityMetadata { LogicalName = "account" });

            for (var run = 0; run < 2; run++)
            {
                var tables = ObjectExplorerHandler.GetTableNodes(cache, "objectexplorer://session/Tables", false).ToArray();
                CollectionAssert.AreEqual(new[] { "account", "archived", "retained", "zeta" }, tables.Select(node => node.Label).ToArray());
                Assert.IsTrue(tables.All(node => node.NodeType == "Table" && !node.IsLeaf && node.Metadata.Schema == "dbo"));
                Assert.AreEqual("objectexplorer://session/Tables/account", tables[0].NodePath);
                Assert.AreEqual(tables[0].NodePath, tables[0].Metadata.Urn);
                Assert.AreEqual("account", tables[0].Metadata.Name);
                var archive = ObjectExplorerHandler.GetTableNodes(cache, "objectexplorer://session/Archive", true).ToArray();
                CollectionAssert.AreEqual(new[] { "archived", "retained" }, archive.Select(node => node.Label).ToArray());
                Assert.IsTrue(archive.All(node => node.Metadata.Schema == "archive"));
            }
            Assert.AreEqual(0, cache.EntityLookups);
            Assert.AreEqual(4, cache.ListLookups);
        }

        [TestMethod]
        public void ColumnsReuseCachedEntityAndPreserveOrderingFilteringAndSyntheticNames()
        {
            var entity = new EntityMetadata { LogicalName = "account" };
            var derived = new StringAttributeMetadata { LogicalName = "derived" };
            typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(derived, "image");
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity, new AttributeMetadata[]
            {
                new StringAttributeMetadata { LogicalName = "zeta" },
                new LookupAttributeMetadata { LogicalName = "customerid", Targets = new[] { "account", "contact" } },
                derived,
                new BooleanAttributeMetadata { LogicalName = "active" },
                new PicklistAttributeMetadata { LogicalName = "category" },
                new StateAttributeMetadata { LogicalName = "statecode" },
                new StatusAttributeMetadata { LogicalName = "statuscode" },
                new LookupAttributeMetadata { LogicalName = "primarycontactid", Targets = new[] { "contact" } },
                new LookupAttributeMetadata { LogicalName = "missingtargets", Targets = null },
            });
            var cache = new MetadataCache(entity);
            for (var run = 0; run < 2; run++)
            {
                var columns = ObjectExplorerHandler.GetColumnNodes(cache, "objectexplorer://session/Tables/account").ToArray();
                CollectionAssert.AreEqual(new[]
                {
                    "active", "activename", "category", "categoryname", "customerid", "customeridname", "customeridtype",
                    "missingtargets", "missingtargetsname", "primarycontactid", "primarycontactidname",
                    "statecode", "statecodename", "statuscode", "statuscodename", "zeta"
                }, columns.Select(node => node.Label).ToArray());
                Assert.IsTrue(columns.All(node => node.IsLeaf && node.NodeType == "Column"));
                foreach (var column in columns)
                {
                    Assert.AreEqual("objectexplorer://session/Tables/account/" + column.Label, column.NodePath);
                    Assert.AreEqual(column.NodePath, column.Metadata.Urn);
                    Assert.AreEqual("account." + column.Label, column.Metadata.Name);
                }
            }
            Assert.AreEqual(2, cache.EntityLookups);
            Assert.AreEqual(0, cache.ListLookups);
        }

        [DataTestMethod]
        [DataRow(ColumnOrdering.Strict)]
        [DataRow(ColumnOrdering.Alphabetical)]
        public void TableScriptsUseReadableAndWritableColumnsAndTypedPlaceholders(ColumnOrdering ordering)
        {
            var entity = new EntityMetadata { LogicalName = "account" };
            var attributes = new AttributeMetadata[]
            {
                new IntegerAttributeMetadata
                {
                    LogicalName = "accountid"
                },
                new IntegerAttributeMetadata
                {
                    LogicalName = "number"
                },
                new IntegerAttributeMetadata
                {
                    LogicalName = "hidden"
                },
                new BooleanAttributeMetadata
                {
                    LogicalName = "active"
                }
            };
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity, attributes);
            for (var index = 0; index < attributes.Length; index++)
            {
                var attribute = attributes[index];
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.ColumnNumber)).SetValue(attribute, index + 1);
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.EntityLogicalName)).SetValue(attribute, "dbo.account");
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForRead)).SetValue(attribute, attribute.LogicalName != "hidden");
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForCreate)).SetValue(attribute, attribute.LogicalName != "hidden");
                typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.IsValidForUpdate)).SetValue(attribute, attribute.LogicalName == "number");
            }
            var cache = new MetadataCache(entity);
            var dataSource = new DataSource { Metadata = cache };

            var select = ObjectExplorerScriptGenerator.Generate(dataSource, ordering, "Table", "dbo", "account", "SELECT");
            var top = ObjectExplorerScriptGenerator.Generate(dataSource, ordering, "Table", "dbo", "account", "SELECT_TOP_1000");
            var insert = ObjectExplorerScriptGenerator.Generate(dataSource, ordering, "Table", "dbo", "account", "INSERT");
            var update = ObjectExplorerScriptGenerator.Generate(dataSource, ordering, "Table", "dbo", "account", "UPDATE");
            var delete = ObjectExplorerScriptGenerator.Generate(dataSource, ordering, "Table", "dbo", "account", "DELETE");

            Assert.AreEqual(ScriptGenerator.GenerateSelectQuery("dbo", entity, null, dataSource, ordering), select);
            Assert.AreEqual(ScriptGenerator.GenerateSelectQuery("dbo", entity, 1000, dataSource, ordering), top);
            Assert.AreEqual(ScriptGenerator.GenerateInsertQuery("dbo", entity, dataSource, ordering), insert);
            Assert.AreEqual(ScriptGenerator.GenerateUpdateQuery("dbo", entity, dataSource, ordering), update);
            Assert.AreEqual(ScriptGenerator.GenerateDeleteQuery("dbo", "account"), delete);
            Assert.AreEqual(ordering == ColumnOrdering.Alphabetical,
                select.IndexOf("active", StringComparison.Ordinal) < select.IndexOf("number", StringComparison.Ordinal));

            StringAssert.Contains(select, "accountid");
            StringAssert.Contains(select, "number");
            StringAssert.Contains(select, "activename");
            Assert.IsFalse(select.Contains("hidden", StringComparison.Ordinal));
            StringAssert.Contains(top, "1000");
            StringAssert.Contains(insert, "accountid");
            StringAssert.Contains(insert, "number");
            StringAssert.Contains(insert, "<number, INT>");
            Assert.IsFalse(insert.Contains("hidden", StringComparison.Ordinal));
            StringAssert.Contains(update, "number");
            StringAssert.Contains(update, "<number, INT>");
            Assert.IsFalse(update.Contains("accountid", StringComparison.Ordinal));
            Assert.IsFalse(update.Contains("hidden", StringComparison.Ordinal));
            StringAssert.Contains(delete, "DELETE FROM dbo.account");
            StringAssert.Contains(delete, "<Search Conditions>");
        }

        [DataTestMethod]
        [DataRow(ColumnOrdering.Strict)]
        [DataRow(ColumnOrdering.Alphabetical)]
        public void SharedMessageScriptsPreserveParameterOrderingTypesAndOutputVariables(ColumnOrdering ordering)
        {
            var message = new Message
            {
                Name = "Example",
                InputParameters = new[]
                {
                    new MessageParameter { Name = "Zeta", Type = typeof(int), Position = 0 },
                    new MessageParameter { Name = "Alpha", Type = typeof(Guid), Position = 1 },
                    new MessageParameter { Name = "PagingInfo", Type = typeof(Microsoft.Xrm.Sdk.Query.PagingInfo), Position = 2 }
                },
                OutputParameters = new[]
                {
                    new MessageParameter { Name = "Result", Type = typeof(int), Position = 0 }
                }
            };
            var dataSource = new DataSource();
            var select = ScriptGenerator.GenerateSelectQuery(message, dataSource, ordering);
            var execute = ScriptGenerator.GenerateExecuteQuery(message, dataSource, ordering);

            StringAssert.Contains(select, "<@Zeta, INT>");
            StringAssert.Contains(select, "<@Alpha, UNIQUEIDENTIFIER>");
            StringAssert.Contains(select, "FROM Example(");
            StringAssert.Contains(execute, "@RC AS INT");
            StringAssert.Contains(execute, "@Zeta AS INT");
            StringAssert.Contains(execute, "@Alpha AS UNIQUEIDENTIFIER");
            StringAssert.Contains(execute, "@Result AS INT");
            StringAssert.Contains(execute, "dbo.Example");
            StringAssert.Contains(execute, "@Result = @Result OUTPUT");
            foreach (var script in new[] { select, execute })
            {
                Assert.IsFalse(script.Contains("PagingInfo", StringComparison.Ordinal));
                Assert.AreEqual(ordering == ColumnOrdering.Alphabetical,
                    script.IndexOf("@Alpha", StringComparison.Ordinal) < script.IndexOf("@Zeta", StringComparison.Ordinal));
            }
        }

        private sealed class MetadataCache : IAttributeMetadataCache
        {
            private readonly EntityMetadata[] _entities;
            public int EntityLookups;
            public int ListLookups;

            public MetadataCache(params EntityMetadata[] entities) => _entities = entities;
            public EntityMetadata this[string name]
            {
                get
                {
                    EntityLookups++;
                    return _entities.Single(entity => entity.LogicalName == name);
                }
            }
            public IEnumerable<EntityMetadata> GetAllEntities()
            {
                ListLookups++;
                return _entities;
            }
            public EntityMetadata this[int otc] => throw new NotSupportedException();
            public string[] RecycleBinEntities => throw new NotSupportedException();
            public bool TryGetValue(string logicalName, out EntityMetadata metadata) => throw new NotSupportedException();
            public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata) => throw new NotSupportedException();
            public string[] TryGetRecycleBinEntities() => throw new NotSupportedException();
        }
    }
}
