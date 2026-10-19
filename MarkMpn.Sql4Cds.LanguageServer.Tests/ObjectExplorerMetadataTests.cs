using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts;
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
            var handler = typeof(WebApiMetadataQuery).Assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.ObjectExplorerHandler", true);
            Assert.AreEqual(expected, handler.GetMethod("IsOnline", BindingFlags.NonPublic | BindingFlags.Static).Invoke(null, new object[] { service }));
        }

        private static NodeInfo[] Nodes(string method, params object[] args)
        {
            var handler = typeof(WebApiMetadataQuery).Assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.ObjectExplorerHandler", true);
            return ((IEnumerable<NodeInfo>)handler.GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static).Invoke(null, args)).ToArray();
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
                var tables = Nodes("GetTableNodes", cache, "objectexplorer://session/Tables", false);
                CollectionAssert.AreEqual(new[] { "account", "archived", "retained", "zeta" }, tables.Select(node => node.Label).ToArray());
                Assert.IsTrue(tables.All(node => node.NodeType == "Table" && !node.IsLeaf && node.Metadata.Schema == "dbo"));
                Assert.AreEqual("objectexplorer://session/Tables/account", tables[0].NodePath);
                Assert.AreEqual(tables[0].NodePath, tables[0].Metadata.Urn);
                Assert.AreEqual("account", tables[0].Metadata.Name);
                var archive = Nodes("GetTableNodes", cache, "objectexplorer://session/Archive", true);
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
                var columns = Nodes("GetColumnNodes", cache, "objectexplorer://session/Tables/account");
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
