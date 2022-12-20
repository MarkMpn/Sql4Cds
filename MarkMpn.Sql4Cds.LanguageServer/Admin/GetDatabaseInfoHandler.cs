using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;
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
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return new ListDatabasesResponse
            {
                DatabaseNames = new[] { con.DataSource.Name }
            };
        }

        public bool HandleChangeDatabase(ChangeDatabaseParams request)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return con.DataSource.Name == request.NewDatabase || request.NewDatabase == null;
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

    /// <summary>
    /// Params for a get database info request
    /// </summar>
    public class GetDatabaseInfoParams
    {
        /// <summary>
        /// Uri identifier for the connection to get the database info for
        /// </summary>
        public string OwnerUri { get; set; }
    }

    /// <summary>
    /// Response object for get database info
    /// </summary>
    public class GetDatabaseInfoResponse
    {
        /// <summary>
        /// The object containing the database info
        /// </summary>
        public DatabaseInfo DatabaseInfo { get; set; }
    }

    /// <summary>
    /// Get database info request mapping
    /// </summary>
    public class GetDatabaseInfoRequest
    {
        public const string MessageName = "admin/getdatabaseinfo";

        public static readonly LspRequest<GetDatabaseInfoParams, GetDatabaseInfoResponse> Type = new LspRequest<GetDatabaseInfoParams, GetDatabaseInfoResponse>(MessageName);
    }

    public class DatabaseInfo
    {
        /// <summary>
        /// Gets or sets the options
        /// </summary>
        public Dictionary<string, object> Options { get; set; }

        public DatabaseInfo()
        {
            Options = new Dictionary<string, object>();
        }

    }

    public class ListDatabasesParams
    {
        /// <summary>
        /// URI of the owner of the connection requesting the list of databases.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// whether to include the details of the databases.
        /// </summary>
        public bool? IncludeDetails { get; set; }
    }

    public class ListDatabasesResponse
    {
        /// <summary>
        /// Gets or sets the list of database names.
        /// </summary>
        public string[] DatabaseNames { get; set; }
    }
    /// <summary>
    /// List databases request mapping entry 
    /// </summary>
    public class ListDatabasesRequest
    {
        public const string MessageName = "connection/listdatabases";

        public static readonly LspRequest<ListDatabasesParams, ListDatabasesResponse> Type = new LspRequest<ListDatabasesParams, ListDatabasesResponse>(MessageName);
    }

    public class ChangeDatabaseParams
    {
        /// <summary>
        /// URI of the owner of the connection requesting the list of databases.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// The database to change to
        /// </summary>
        public string NewDatabase { get; set; }
    }

    /// <summary>
    /// List databases request mapping entry 
    /// </summary>
    public class ChangeDatabaseRequest
    {
        public const string MessageName = "connection/changedatabase";

        public static readonly LspRequest<ChangeDatabaseParams, bool> Type = new LspRequest<ChangeDatabaseParams, bool>(MessageName);
    }

    public class MetadataQueryParams
    {
        public string OwnerUri { get; set; }
    }

    public class MetadataQueryResult
    {
        public ObjectMetadata[] Metadata { get; set; }
    }

    public class MetadataListRequest
    {
        public const string MessageName = "metadata/list";

        public static readonly LspRequest<MetadataQueryParams, MetadataQueryResult> Type = new LspRequest<MetadataQueryParams, MetadataQueryResult>(MessageName);
    }
}
