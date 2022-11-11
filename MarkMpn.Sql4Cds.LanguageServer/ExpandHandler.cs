using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MediatR;
using Microsoft.Xrm.Sdk;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Server;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    [Method("objectexplorer/expand")]
    [Serial]
    class ExpandHandler : IRequestHandler<ExpandParams, bool>, IJsonRpcHandler
    {
        private readonly ILanguageServerFacade _lsp;
        private readonly ConnectionManager _connectionManager;

        public ExpandHandler(ILanguageServerFacade lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public async Task<bool> Handle(ExpandParams request, CancellationToken cancellationToken)
        {
            var session = _connectionManager.GetConnection(request.SessionId);

            if (session == null)
                return false;

            var url = new Uri(request.NodePath);
            var nodes = new List<NodeInfo>();

            if (url.AbsolutePath == "/")
            {
                nodes.Add(new NodeInfo
                {
                    IsLeaf = false,
                    Label = "Tables",
                    NodePath = request.NodePath + "/Tables",
                    NodeType = "Folder",
                });
                nodes.Add(new NodeInfo
                {
                    IsLeaf = false,
                    Label = "Metadata",
                    NodePath = request.NodePath + "/Metadata",
                    NodeType = "Folder"
                });
                nodes.Add(new NodeInfo
                {
                    IsLeaf = false,
                    Label = "Programmability",
                    NodePath = request.NodePath + "/Programmability",
                    NodeType = "Folder"
                });
            }
            else if (url.AbsolutePath == "/Programmability")
            {
                nodes.Add(new NodeInfo
                {
                    IsLeaf = false,
                    Label = "Table-valued Functions",
                    NodePath = request.NodePath + "/TVF",
                    NodeType = "Folder",
                });
                nodes.Add(new NodeInfo
                {
                    IsLeaf = false,
                    Label = "Stored Procedures",
                    NodePath = request.NodePath + "/SProcs",
                    NodeType = "Folder"
                });
            }
            else if (url.AbsolutePath == "/Tables")
            {
                using (var cmd = session.Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT logicalname FROM metadata.entity ORDER BY 1";

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var logicalName = reader.GetString(0);

                            nodes.Add(new NodeInfo
                            {
                                IsLeaf = false,
                                Label = logicalName,
                                NodePath = request.NodePath + "/" + logicalName,
                                NodeType = "Table",
                                Metadata = new ObjectMetadata
                                {
                                    Urn = request.NodePath + "/" + logicalName,
                                    MetadataType = MetadataType.Table,
                                    Name = logicalName
                                }
                            });
                        }
                    }
                }
            }
            else if (url.AbsolutePath.StartsWith("/Tables/") && url.AbsolutePath.Split('/').Length == 3)
            {
                var tableName = url.AbsolutePath.Split('/')[2];

                using (var cmd = session.Connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT logicalname, attributetypename, targets FROM metadata.attribute WHERE entitylogicalname = @entity AND attributeof IS NULL ORDER BY 1";
                    var param = cmd.CreateParameter();
                    param.ParameterName = "@entity";
                    param.Value = tableName;
                    cmd.Parameters.Add(param);

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var logicalName = reader.GetString(0);
                            var attributeTypeName = reader.GetString(1);
                            var targets = await reader.IsDBNullAsync(2) ? null : reader.GetString(2);

                            nodes.Add(new NodeInfo
                            {
                                IsLeaf = true,
                                Label = logicalName,
                                NodePath = request.NodePath + "/" + logicalName,
                                NodeType = "Column",
                                Metadata = new ObjectMetadata
                                {
                                    Urn = request.NodePath + "/" + logicalName,
                                    Name = tableName + "." + logicalName
                                }
                            });

                            if (attributeTypeName == "BooleanType" || attributeTypeName == "PicklistType" || attributeTypeName == "StateType" || attributeTypeName == "StatusType" || attributeTypeName == "LookupType" || attributeTypeName == "OwnerType" || attributeTypeName == "CustomerType")
                            {
                                nodes.Add(new NodeInfo
                                {
                                    IsLeaf = true,
                                    Label = logicalName + "name",
                                    NodePath = request.NodePath + "/" + logicalName + "name",
                                    NodeType = "Column",
                                    Metadata = new ObjectMetadata
                                    {
                                        Urn = request.NodePath + "/" + logicalName + "name",
                                        Name = tableName + "." + logicalName + "name"
                                    }
                                });
                            }

                            if (!String.IsNullOrEmpty(targets) && targets.Split(',').Length > 1)
                            {
                                nodes.Add(new NodeInfo
                                {
                                    IsLeaf = true,
                                    Label = logicalName + "type",
                                    NodePath = request.NodePath + "/" + logicalName + "type",
                                    NodeType = "Column",
                                    Metadata = new ObjectMetadata
                                    {
                                        Urn = request.NodePath + "/" + logicalName + "type",
                                        Name = tableName + "." + logicalName + "type"
                                    }
                                });
                            }
                        }
                    }
                }
            }
            else if (url.AbsolutePath == "/Metadata")
            {
                var metadata = MetaMetadataCache.GetMetadata();

                foreach (var table in metadata.OrderBy(t => t.LogicalName))
                {
                    nodes.Add(new NodeInfo
                    {
                        IsLeaf = false,
                        Label = table.LogicalName,
                        NodePath = request.NodePath + "/" + table.LogicalName,
                        NodeType = "Table",
                        Metadata = new ObjectMetadata
                        {
                            Urn = request.NodePath + "/" + table.LogicalName,
                            MetadataType = MetadataType.Table,
                            Name = table.LogicalName
                        }
                    });
                }
            }
            else if (url.AbsolutePath.StartsWith("/Metadata/") && url.AbsolutePath.Split('/').Length == 3)
            {
                var tableName = url.AbsolutePath.Split('/')[2];
                var metadata = MetaMetadataCache.GetMetadata();
                var table = metadata.Single(t => t.LogicalName == tableName);

                foreach (var attribute in table.Attributes)
                {
                    nodes.Add(new NodeInfo
                    {
                        IsLeaf = true,
                        Label = attribute.LogicalName,
                        NodePath = request.NodePath + "/" + attribute.LogicalName,
                        NodeType = "Column",
                        Metadata = new ObjectMetadata
                        {
                            Urn = request.NodePath + "/" + attribute.LogicalName,
                            Name = tableName + "." + attribute.LogicalName
                        }
                    });
                }
            }
            else if (url.AbsolutePath == "/Programmability/TVF")
            {
                foreach (var msg in session.DataSource.MessageCache.GetAllMessages().OrderBy(m => m.Name))
                {
                    if (msg.IsValidAsTableValuedFunction())
                    {
                        nodes.Add(new NodeInfo
                        {
                            IsLeaf = true,
                            Label = msg.Name,
                            NodePath = request.NodePath + "/" + msg.Name,
                            NodeType = "TableValuedFunction",
                            Metadata = new ObjectMetadata
                            {
                                Urn = request.NodePath + "/" + msg.Name,
                                MetadataType = MetadataType.Function,
                                Name = msg.Name
                            }
                        });
                    }
                }
            }
            else if (url.AbsolutePath == "/Programmability/SProcs")
            {
                foreach (var msg in session.DataSource.MessageCache.GetAllMessages().OrderBy(m => m.Name))
                {
                    if (msg.IsValidAsStoredProcedure())
                    {
                        nodes.Add(new NodeInfo
                        {
                            IsLeaf = true,
                            Label = msg.Name,
                            NodePath = request.NodePath + "/" + msg.Name,
                            NodeType = "StoredProcedure",
                            Metadata = new ObjectMetadata
                            {
                                Urn = request.NodePath + "/" + msg.Name,
                                MetadataType = MetadataType.SProc,
                                Name = msg.Name
                            }
                        });
                    }
                }
            }

            _lsp.SendNotification("objectexplorer/expandCompleted", new ExpandResponse
            {
                SessionId = request.SessionId,
                NodePath = request.NodePath,
                Nodes = nodes.ToArray()
            });

            return true;
        }
    }

    /// <summary>
    /// Parameters to the <see cref="ExpandRequest"/>.
    /// </summary>
    [Method("objectexplorer/expand")]
    [Serial]
    public class ExpandParams : IRequest<bool>
    {
        /// <summary>
        /// The Id returned from a <see cref="CreateSessionRequest"/>. This
        /// is used to disambiguate between different trees. 
        /// </summary>
        public string SessionId { get; set; }

        /// <summary>
        /// Path identifying the node to expand. See <see cref="NodeInfo.NodePath"/> for details
        /// </summary>
        public string NodePath { get; set; }
    }

    /// <summary>
    /// Information returned from a <see cref="ExpandRequest"/>.
    /// </summary>
    public class ExpandResponse
    {
        /// <summary>
        /// Unique ID to use when sending any requests for objects in the
        /// tree under the node
        /// </summary>
        public string SessionId { get; set; }

        /// <summary>
        /// Information describing the expanded nodes in the tree
        /// </summary>
        public NodeInfo[] Nodes { get; set; }

        /// <summary>
        /// Path identifying the node to expand. See <see cref="NodeInfo.NodePath"/> for details
        /// </summary>
        public string NodePath { get; set; }

        /// <summary>
        /// Error message returned from the engine for a object explorer expand failure reason, if any.
        /// </summary>
        public string ErrorMessage { get; set; }
    }
}
