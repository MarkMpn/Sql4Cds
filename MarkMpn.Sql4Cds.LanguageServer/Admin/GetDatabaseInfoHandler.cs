using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin
{
    class GetDatabaseInfoHandler : IJsonRpcMethodHandler
    {
        private readonly ConnectionManager _connectionManager;

        public GetDatabaseInfoHandler(ConnectionManager connectionManager)
        {
            _connectionManager = connectionManager;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(GetDatabaseInfoRequest.Type, HandleGetDatabaseInfo);
            lsp.AddHandler(ListDatabasesRequest.Type, HandleListDatabases);
            lsp.AddHandler(ChangeDatabaseRequest.Type, HandleChangeDatabase);
            lsp.AddHandler(MetadataListRequest.Type, HandleMetadataList);
        }

        public GetDatabaseInfoResponse HandleGetDatabaseInfo(GetDatabaseInfoParams request)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return new GetDatabaseInfoResponse
            {
                DatabaseInfo = new DatabaseInfo
                {
                    Options = new Dictionary<string, object>
                    {
                        ["dbname"] = con.DataSource.Name,
                        ["url"] = con.DataSource.ServerName,
                        ["username"] = con.DataSource.Username
                    }
                }
            };
        }

        public ListDatabasesResponse HandleListDatabases(ListDatabasesParams request)
        {
            var connections = _connectionManager.GetAllConnections();

            return new ListDatabasesResponse
            {
                DatabaseNames = connections.Keys.Order().ToArray()
            };
        }

        public bool HandleChangeDatabase(ChangeDatabaseParams request)
        {
            return _connectionManager.ChangeConnection(request.OwnerUri, request.NewDatabase);
        }

        public MetadataQueryResult HandleMetadataList(MetadataQueryParams request)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);
            var metadata = new List<ObjectMetadata>();

            metadata.Add(new ObjectMetadata { MetadataType = MetadataType.Schema, MetadataTypeName = "Schema", Name = "dbo" });
            metadata.Add(new ObjectMetadata { MetadataType = MetadataType.Schema, MetadataTypeName = "Schema", Name = "metadata" });

            using (var cmd = con.Connection.CreateCommand())
            {
                cmd.CommandText = "SELECT logicalname FROM metadata.entity ORDER BY 1";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var logicalName = reader.GetString(0);

                        metadata.Add(new ObjectMetadata { MetadataType = MetadataType.Table, MetadataTypeName = "Table", Name = logicalName, Schema = "dbo" });
                    }
                }
            }

            foreach (var table in MetaMetadataCache.GetMetadata().OrderBy(t => t.LogicalName))
            {
                metadata.Add(new ObjectMetadata { MetadataType = MetadataType.Table, MetadataTypeName = "Table", Name = table.LogicalName, Schema = "metadata" });
            }

            foreach (var msg in con.DataSource.MessageCache.GetAllMessages().OrderBy(m => m.Name))
            {
                if (msg.IsValidAsTableValuedFunction())
                {
                    metadata.Add(new ObjectMetadata { MetadataType = MetadataType.Function, MetadataTypeName = "Function", Name = msg.Name, Schema = "dbo" });
                }

                if (msg.IsValidAsStoredProcedure())
                {
                    metadata.Add(new ObjectMetadata { MetadataType = MetadataType.SProc, MetadataTypeName = "SProc", Name = msg.Name, Schema = "dbo" });
                }
            }

            return new MetadataQueryResult
            {
                Metadata = metadata.ToArray()
            };
        }
    }
}
