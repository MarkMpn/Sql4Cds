using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using OmniSharp.Extensions.JsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class GetDatabaseInfoHandler : IRequestHandler<GetDatabaseInfoParams, GetDatabaseInfoResponse>, IRequestHandler<ListDatabasesParams, ListDatabasesResponse>, IRequestHandler<ChangeDatabaseParams, bool>, IRequestHandler<MetadataQueryParams, MetadataQueryResult>, IJsonRpcHandler
    {
        private readonly ConnectionManager _connectionManager;

        public GetDatabaseInfoHandler(ConnectionManager connectionManager)
        {
            _connectionManager = connectionManager;
        }

        public Task<GetDatabaseInfoResponse> Handle(GetDatabaseInfoParams request, CancellationToken cancellationToken)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return Task.FromResult(new GetDatabaseInfoResponse
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
            });
        }

        public Task<ListDatabasesResponse> Handle(ListDatabasesParams request, CancellationToken cancellationToken)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return Task.FromResult(new ListDatabasesResponse
            {
                DatabaseNames = new[] { con.DataSource.Name }
            });
        }

        public Task<bool> Handle(ChangeDatabaseParams request, CancellationToken cancellationToken)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return Task.FromResult(con.DataSource.Name == request.NewDatabase || request.NewDatabase == null);
        }

        public Task<MetadataQueryResult> Handle(MetadataQueryParams request, CancellationToken cancellationToken)
        {
            var con = _connectionManager.GetConnection(request.OwnerUri);

            return Task.FromResult(new MetadataQueryResult
            {
                Metadata = new[] { new ObjectMetadata { MetadataType = MetadataType.Table, MetadataTypeName = "Table", Name = "account", Schema = "dbo" } }
            });
        }
    }

    /// <summary>
    /// Params for a get database info request
    /// </summar>
    [Method("admin/getdatabaseinfo")]
    [Serial]
    public class GetDatabaseInfoParams : IRequest<GetDatabaseInfoResponse>
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

    [Method("connection/listdatabases")]
    [Serial]
    public class ListDatabasesParams : IRequest<ListDatabasesResponse>
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

    [Method("connection/changedatabase")]
    [Serial]
    public class ChangeDatabaseParams : IRequest<bool>
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

    [Method("metadata/list")]
    [Serial]
    public class MetadataQueryParams : IRequest<MetadataQueryResult>
    {
        public string OwnerUri { get; set; }
    }

    public class MetadataQueryResult
    {
        public ObjectMetadata[] Metadata { get; set; }
    }
}
