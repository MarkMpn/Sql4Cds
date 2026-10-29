using System;
using System.Runtime.CompilerServices;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class ClientInfoTests
    {
        [DataTestMethod]
        [DataRow("VS Code", "9.4.0", "VS Code")]
        [DataRow("Power Platform ToolBox", "9.4.0-beta.1", "Power Platform ToolBox")]
        [DataRow("Another LSP client", null, "Another LSP client")]
        [DataRow(null, null, "SQL 4 CDS LSP")]
        [DataRow("", null, "SQL 4 CDS LSP")]
        [DataRow("  ", null, "SQL 4 CDS LSP")]
        public void InitializeClientInfoDeterminesApplicationNameForConnections(string name, string version, string expected)
        {
            var manager = new ConnectionManager(null, null, null);
            var handler = new CapabilitiesHandler(null, new PptbRpcChunking(), new QueryEventStream(), manager);
            var request = JsonConvert.DeserializeObject<ClientInitializeParams>(JsonConvert.SerializeObject(new
            {
                capabilities = new { },
                clientInfo = name == null ? null : new { name, version }
            }));
            Assert.AreEqual(version, request.ClientInfo?.Version);
            handler.HandleInitialize(request);
            Assert.AreEqual(expected, manager.ApplicationName);

            // Avoid authentication and metadata requests when creating test connections.
            var source = (DataSourceWithInfo)RuntimeHelpers.GetUninitializedObject(typeof(DataSourceWithInfo));
            source.Name = "test";
            source.DefaultCollation = Collation.USEnglish;
            manager._dataSources[source.Name] = source;
            foreach (var owner in new[] { "test://editor/1", "test://editor/2" })
            {
                manager._connectedDataSource[owner] = source.Name;
                var connection = manager.GetConnection(owner).Connection;
                using (connection)
                {
                    Assert.AreEqual(expected, connection.ApplicationName);
                    Assert.AreSame(connection, manager.GetConnection(owner).Connection);
                }
            }
        }
    }
}
