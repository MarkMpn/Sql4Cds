using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer
{
    class ObjectExplorerHandler : IJsonRpcMethodHandler
    {
        private readonly JsonRpc _lsp;
        private readonly ConnectionManager _connectionManager;

        public ObjectExplorerHandler(JsonRpc lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(CreateSessionRequest.Type, HandleCreateSession);
            lsp.AddHandler(ExpandRequest.Type, HandleExpand);
            lsp.AddHandler(RefreshRequest.Type, HandleRefresh);
        }

        public CreateSessionResponse HandleCreateSession(ConnectionDetails request)
        {
            var session = _connectionManager.Connect(request, null);

            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                _ = _lsp.NotifyAsync(CreateSessionCompleteNotification.Type, new SessionCreatedParameters
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

            return new CreateSessionResponse { SessionId = session.SessionId };
        }

        public bool HandleExpand(ExpandParams request)
        {
            var session = _connectionManager.GetConnection(request.SessionId);

            if (session == null)
                return false;

            _ = Task.Run(async () =>
            {
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
                                        Schema = "dbo",
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
                                Schema = "metadata",
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
                                    Schema = "dbo",
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
                                    Schema = "dbo",
                                    Name = msg.Name
                                }
                            });
                        }
                    }
                }

                await _lsp.NotifyAsync(ExpandCompleteNotification.Type, new ExpandResponse
                {
                    SessionId = request.SessionId,
                    NodePath = request.NodePath,
                    Nodes = nodes.ToArray()
                });
            });

            return true;
        }

        private bool HandleRefresh(RefreshParams args)
        {
            return HandleExpand(args);
        }
    }
}
