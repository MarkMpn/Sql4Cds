using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Server;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    [Method("connection/connect")]
    [Serial]
    class ConnectionHandler : IRequestHandler<ConnectRequest, bool>, IJsonRpcHandler
    {
        private readonly ILanguageServerFacade _lsp;
        private readonly ConnectionManager _connectionManager;

        public ConnectionHandler(ILanguageServerFacade lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public Task<bool> Handle(ConnectRequest request, CancellationToken cancellationToken)
        {
            try
            {
                var session = _connectionManager.Connect(request.Connection, request.OwnerUri);

                _lsp.SendNotification("connection/complete", new ConnectionCompleteParams
                {
                    OwnerUri = request.OwnerUri,
                    ConnectionId = session.SessionId,
                    ServerInfo = new ServerInfo
                    {
                        MachineName = session.DataSource.ServerName,
                        Options = new Dictionary<string, object>
                        {
                            ["server"] = session.DataSource.ServerName,
                            ["orgVersion"] = session.DataSource.Version,
                            ["edition"] = session.DataSource.ServerName.EndsWith(".dynamics.com") ? "Online" : "On-Premises"
                        }
                    },
                    Type = request.Type,
                    ConnectionSummary = new ConnectionSummary
                    {
                        ServerName = session.DataSource.ServerName,
                        DatabaseName = session.DataSource.Name,
                        UserName = session.DataSource.Username
                    }
                });
                return Task.FromResult(true);
            }
            catch
            {
                return Task.FromResult(false);
            }
        }
    }

    [Method("connection/connect", Direction.ClientToServer)]
    [Serial]
    class ConnectRequest : IRequest<bool>
    {
        /// <summary>
        /// A URI identifying the owner of the connection. This will most commonly be a file in the workspace
        /// or a virtual file representing an object in a database.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Contains the required parameters to initialize a connection to a database.
        /// A connection will identified by its server name, database name and user name.
        /// This may be changed in the future to support multiple connections with different
        /// connection properties to the same database.
        /// </summary>
        public ConnectionDetails Connection { get; set; }

        /// <summary>
        /// The type of this connection. By default, this is set to ConnectionType.Default.
        /// </summary>
        public string Type { get; set; } = ConnectionType.Default;

        /// <summary>
        /// The porpose of the connection to keep track of open connections
        /// </summary>
        public string Purpose { get; set; } = ConnectionType.GeneralConnection;
    }

    [Method("objectexplorer/createsession", Direction.ClientToServer)]
    [Serial]
    class ConnectionDetails : IRequest<CreateSessionResponse>
    {/*
        public string ConnectionString { get; set; }

        public string AuthenticationType { get; set; }

        public string url { get; set; }

        public string User { get; set; }

        public string Password { get; set; }

        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public string AzureAccountToken { get; set; }*/

        public Dictionary<string, object> Options { get; set; }
    }
    
    /// <summary>
    /// String constants that represent connection types. 
    /// 
    /// Default: Connection used by the editor. Opened by the editor upon the initial connection. 
    /// Query: Connection used for executing queries. Opened when the first query is executed.
    /// </summary>
    public static class ConnectionType
    {
        public const string Default = "Default";
        public const string Query = "Query";
        public const string Edit = "Edit";
        public const string ObjectExplorer = "ObjectExplorer";
        public const string Dashboard = "Dashboard";
        public const string GeneralConnection = "GeneralConnection";
    }
    public class ConnectionCompleteParams
    {
        /// <summary>
        /// A URI identifying the owner of the connection. This will most commonly be a file in the workspace
        /// or a virtual file representing an object in a database.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// A GUID representing a unique connection ID
        /// </summary>
        public string ConnectionId { get; set; }

        /// <summary>
        /// Gets or sets any detailed connection error messages.
        /// </summary>
        public string Messages { get; set; }

        /// <summary>
        /// Error message returned from the engine for a connection failure reason, if any.
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Error number returned from the engine for connection failure reason, if any.
        /// </summary>
        public int ErrorNumber { get; set; }

        /// <summary>
        /// Information about the connected server.
        /// </summary>
        public ServerInfo ServerInfo { get; set; }

        /// <summary>
        /// Gets or sets the actual Connection established, including Database Name
        /// </summary>
        public ConnectionSummary ConnectionSummary { get; set; }

        /// <summary>
        /// The type of connection that this notification is for
        /// </summary>
        public string Type { get; set; } = ConnectionType.Default;
    }
    /// <summary>
    /// Contract for information on the connected SQL Server instance.
    /// </summary>
    public class ServerInfo
    {
        /// <summary>
        /// The major version of the SQL Server instance.
        /// </summary>
        public int ServerMajorVersion { get; set; }

        /// <summary>
        /// The minor version of the SQL Server instance.
        /// </summary>
        public int ServerMinorVersion { get; set; }

        /// <summary>
        /// The build of the SQL Server instance.
        /// </summary>
        public int ServerReleaseVersion { get; set; }

        /// <summary>
        /// The ID of the engine edition of the SQL Server instance.
        /// </summary>
        public int EngineEditionId { get; set; }

        /// <summary>
        /// String containing the full server version text.
        /// </summary>
        public string ServerVersion { get; set; }

        /// <summary>
        /// String describing the product level of the server.
        /// </summary>
        public string ServerLevel { get; set; }

        /// <summary>
        /// The edition of the SQL Server instance.
        /// </summary>
        public string ServerEdition { get; set; }

        /// <summary>
        /// Whether the SQL Server instance is running in the cloud (Azure) or not.
        /// </summary>
        public bool IsCloud { get; set; }

        /// <summary>
        /// The version of Azure that the SQL Server instance is running on, if applicable.
        /// </summary>
        public int AzureVersion { get; set; }

        /// <summary>
        /// The Operating System version string of the machine running the SQL Server instance.
        /// </summary>
        public string OsVersion { get; set; }

        /// <summary>
        /// The Operating System version string of the machine running the SQL Server instance.
        /// </summary>
        public string MachineName { get; set; }

        /// <summary>
        /// The CPU count of the host running the server.
        /// </summary>
        public Nullable<int> CpuCount;

        /// <summary>
        /// The physical memory of the host running the server in MBs.
        /// </summary>
        public Nullable<int> PhysicalMemoryInMB;

        /// <summary>
        /// Server options
        /// </summary>
        public Dictionary<string, object> Options { get; set; }
    }
    /// <summary>
    /// Provides high level information about a connection.
    /// </summary>
    public class ConnectionSummary 
    {
        /// <summary>
        /// Gets or sets the connection server name
        /// </summary>
        public virtual string ServerName { get; set; }

        /// <summary>
        /// Gets or sets the connection database name
        /// </summary>
        public virtual string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the connection user name
        /// </summary>
        public virtual string UserName { get; set; }
    }
}
