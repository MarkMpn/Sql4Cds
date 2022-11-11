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
    [Method("objectexplorer/createsession")]
    [Serial]
    class CreateSessionHandler : IRequestHandler<ConnectionDetails, CreateSessionResponse>, IJsonRpcHandler
    {
        private readonly ILanguageServerFacade _lsp;
        private readonly ConnectionManager _connectionManager;

        public CreateSessionHandler(ILanguageServerFacade lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public Task<CreateSessionResponse> Handle(ConnectionDetails request, CancellationToken cancellationToken)
        {
            var session = _connectionManager.Connect(request);

            Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                _lsp.SendNotification("objectexplorer/sessioncreated", new SessionCreatedParameters
                {
                    Success = true,
                    SessionId = session.SessionId,
                    RootNode = new NodeInfo
                    {
                        IsLeaf = false,
                        Label = "SERVERNODE",
                        NodePath = "objectexplorer://" + session.SessionId,
                        NodeType = "Server",
                        Metadata = new ObjectMetadata
                        {
                            Urn = "objectexplorer://" + session.SessionId,
                            MetadataTypeName = "Database",
                            Name = "SERVERDB"
                        }
                    }
                });
            });
            return Task.FromResult(new CreateSessionResponse { SessionId = session.SessionId });
        }
    }

    /// <summary>
    /// Information returned from a <see cref="CreateSessionRequest"/>.
    /// Contains success information, a <see cref="SessionId"/> to be used when
    /// requesting expansion of nodes, and a root node to display for this area.
    /// </summary>
    public class CreateSessionResponse
    {
        /// <summary>
        /// Unique ID to use when sending any requests for objects in the
        /// tree under the node
        /// </summary>
        public string SessionId { get; set; }

    }

    /// <summary>
    /// Information returned from a <see cref="CreateSessionRequest"/>.
    /// Contains success information, a <see cref="SessionId"/> to be used when
    /// requesting expansion of nodes, and a root node to display for this area.
    /// </summary>
    public class SessionCreatedParameters
    {
        /// <summary>
        /// Boolean indicating if the connection was successful
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Unique ID to use when sending any requests for objects in the
        /// tree under the node
        /// </summary>
        public string SessionId { get; set; }

        /// <summary>
        /// Information describing the base node in the tree
        /// </summary>
        public NodeInfo RootNode { get; set; }


        /// <summary>
        /// Error message returned from the engine for a object explorer session failure reason, if any.
        /// </summary>
        public string ErrorMessage { get; set; }
    }
    /// <summary>
    /// Information describing a Node in the Object Explorer tree. 
    /// Contains information required to display the Node to the user and
    /// to know whether actions such as expanding children is possible
    /// the node 
    /// </summary>
    public class NodeInfo
    {
        /// <summary>
        /// Path identifying this node: for example a table will be at ["server", "database", "tables", "tableName"].
        /// This enables rapid navigation of the tree without the need for a global registry of elements.
        /// The path functions as a unique ID and is used to disambiguate the node when sending requests for expansion.
        /// A common ID is needed since processes do not share address space and need a unique identifier
        /// </summary>
        public string NodePath { get; set; }

        /// <summary>
        /// The type of the node - for example Server, Database, Folder, Table
        /// </summary>
        public string NodeType { get; set; }

        /// <summary>
        /// Label to display to the user, describing this node.
        /// </summary>
        public string Label { get; set; }

        /// <summary>
        /// Node Sub type - for example a key can have type as "Key" and sub type as "PrimaryKey"
        /// </summary>
        public string NodeSubType { get; set; }

        /// <summary>
        /// Node status - for example login can be disabled/enabled
        /// </summary>
        public string NodeStatus { get; set; }

        /// <summary>
        /// Is this a leaf node (in which case no children can be generated) or
        /// is it expandable?
        /// </summary>
        public bool IsLeaf { get; set; }

        /// <summary>
        /// Object Metadata for smo objects to be used for scripting
        /// </summary>
        public ObjectMetadata Metadata { get; set; }

        /// <summary>
        /// Error message returned from the engine for a object explorer node failure reason, if any.
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// The object type of the node. e.g. Database, Server, Tables...
        /// </summary>
        public string ObjectType { get; set; }
    }

    /// <summary>
    /// Metadata type enumeration
    /// </summary>
    public enum MetadataType
    {
        Table = 0,
        View = 1,
        SProc = 2,
        Function = 3,
        Schema = 4,
        Database = 5
    }

    /// <summary>
    /// Object metadata information
    /// </summary>
    public class ObjectMetadata
    {
        public MetadataType MetadataType { get; set; }

        public string MetadataTypeName { get; set; }

        public string Schema { get; set; }

        public string Name { get; set; }

        public string ParentName { get; set; }

        public string ParentTypeName { get; set; }

        public string Urn { get; set; }
    }
}
