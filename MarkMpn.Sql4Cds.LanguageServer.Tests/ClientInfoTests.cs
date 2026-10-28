using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
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
            var assembly = typeof(WebApiMetadataQuery).Assembly;
            var managerType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.ConnectionManager", true);
            var manager = Activator.CreateInstance(managerType, new object[] { null, null, null });
            var stream = Activator.CreateInstance(assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.QueryExecution.QueryEventStream", true));
            var chunking = Activator.CreateInstance(assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Pptb.PptbRpcChunking", true));
            var handlerType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Capabilities.CapabilitiesHandler", true);
            var handler = Activator.CreateInstance(handlerType, new[] { null, chunking, stream, manager });
            var request = JsonConvert.DeserializeObject<ClientInitializeParams>(JsonConvert.SerializeObject(new
            {
                capabilities = new { },
                clientInfo = name == null ? null : new { name, version }
            }));
            Assert.AreEqual(version, request.ClientInfo?.Version);
            handlerType.GetMethod("HandleInitialize", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(handler, new object[] { request });
            Assert.AreEqual(expected, managerType.GetProperty("ApplicationName").GetValue(manager));

            // Avoid authentication and metadata requests when creating test connections.
            var sourceType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.DataSourceWithInfo", true);
            var source = (DataSource)RuntimeHelpers.GetUninitializedObject(sourceType);
            source.Name = "test";
            var collation = typeof(Collation).GetProperty("USEnglish", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
            typeof(DataSource).GetProperty("DefaultCollation", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(source, collation);
            var sources = (ConcurrentDictionary<string, DataSource>)managerType.GetField("_dataSources",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(manager);
            sources[source.Name] = source;
            var owners = (ConcurrentDictionary<string, string>)managerType.GetField("_connectedDataSource",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(manager);
            foreach (var owner in new[] { "test://editor/1", "test://editor/2" })
            {
                owners[owner] = source.Name;
                var session = managerType.GetMethod("GetConnection").Invoke(manager, new object[] { owner });
                var connection = (Sql4CdsConnection)session.GetType().GetProperty("Connection").GetValue(session);
                using (connection)
                {
                    Assert.AreEqual(expected, connection.ApplicationName);
                    var repeatedSession = managerType.GetMethod("GetConnection").Invoke(manager, new object[] { owner });
                    Assert.AreSame(connection, repeatedSession.GetType().GetProperty("Connection").GetValue(repeatedSession));
                }
            }
        }
    }
}
