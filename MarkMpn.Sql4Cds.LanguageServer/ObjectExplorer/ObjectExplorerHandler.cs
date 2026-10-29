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
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk.Metadata;
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
            lsp.AddAsyncHandler(CreateSessionRequest.Type, request => Task.Run(() => HandleCreateSession(request)));
            lsp.AddHandler(ExpandRequest.Type, HandleExpand);
            lsp.AddHandler(RefreshRequest.Type, HandleRefresh);
            lsp.AddHandler(CloseSessionRequest.Type, HandleCloseSession);
        }

        public CreateSessionResponse HandleCreateSession(ConnectionDetails request)
        {
            var session = _connectionManager.Connect(request, null);

            _ = Task.Run(async () =>
            {
                await _lsp.NotifyAsync(CreateSessionCompleteNotification.Type, new SessionCreatedParameters
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

        public CloseSessionResponse HandleCloseSession(CloseSessionParams request)
        {
            _connectionManager.Disconnect(request.SessionId);

            _ = _lsp.NotifyAsync(SessionDisconnectedNotification.Type, new SessionDisconnectedParameters
            {
                SessionId = request.SessionId,
                Success = true,
                ErrorMessage = null
            });

            return new CloseSessionResponse
            {
                SessionId = request.SessionId,
                Success = true
            };
        }

        public bool HandleExpand(ExpandParams request)
        {
            var session = _connectionManager.GetConnection(request.SessionId);

            if (session == null)
                return false;

            _ = Task.Run(async () =>
            {
                try
                {
                    var url = new Uri(request.NodePath);
                    var nodes = new List<NodeInfo>();
                    if ((url.AbsolutePath == "/Tables" || url.AbsolutePath == "/Archive") &&
                        session.DataSource.Metadata is CachedMetadata cachedMetadata)
                        await cachedMetadata.EnsureAllEntitiesLoadedAsync();

                    if (url.AbsolutePath == "/")
                    {
                        nodes.Add(new NodeInfo
                        {
                            IsLeaf = false,
                            Label = "Tables",
                            NodePath = request.NodePath + "/Tables",
                            NodeType = "Folder",
                        });

                        if (IsOnline(session.DataSource.Connection))
                        {
                            nodes.Add(new NodeInfo
                            {
                                IsLeaf = false,
                                Label = "Long Term Retention",
                                NodePath = request.NodePath + "/Archive",
                                NodeType = "Folder",
                            });
                        }

                        if (session.DataSource.Metadata.RecycleBinEntities != null)
                        {
                            nodes.Add(new NodeInfo
                            {
                                IsLeaf = false,
                                Label = "Recycle Bin",
                                NodePath = request.NodePath + "/Bin",
                                NodeType = "Folder",
                            });
                        }

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
                    else if (url.AbsolutePath == "/Tables" || url.AbsolutePath == "/Archive")
                    {
                        nodes.AddRange(GetTableNodes(session.DataSource.Metadata, request.NodePath, url.AbsolutePath == "/Archive"));
                    }
                    else if (url.AbsolutePath == "/Bin")
                    {
                        foreach (var entity in session.DataSource.Metadata.RecycleBinEntities)
                        {
                            nodes.Add(new NodeInfo
                            {
                                IsLeaf = false,
                                Label = entity,
                                NodePath = request.NodePath + "/" + entity,
                                NodeType = "Table",
                                Metadata = new ObjectMetadata
                                {
                                    Urn = request.NodePath + "/" + entity,
                                    MetadataType = MetadataType.Table,
                                    Schema = "bin",
                                    Name = entity
                                }
                            });
                        }
                    }
                    else if (url.AbsolutePath.StartsWith("/Tables/") && url.AbsolutePath.Split('/').Length == 3)
                    {
                        nodes.AddRange(GetColumnNodes(session.DataSource.Metadata, request.NodePath));
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
                        foreach (var msg in session.DataSource.MessageCache.GetAllMessages(false).OrderBy(m => m.Name))
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
                        foreach (var msg in session.DataSource.MessageCache.GetAllMessages(false).OrderBy(m => m.Name))
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
                }
                catch (Exception ex)
                {
                    await _lsp.NotifyAsync(ExpandCompleteNotification.Type, new ExpandResponse
                    {
                        SessionId = request.SessionId,
                        NodePath = request.NodePath,
                        ErrorMessage = ex.Message
                    });
                }
            });

            return true;
        }

        internal static bool IsOnline(Microsoft.Xrm.Sdk.IOrganizationService connection)
        {
            var uri = connection is ServiceClient svc ? svc.ConnectedOrgUriActual : (connection as IServiceClient)?.Uri;
            return uri?.Host.EndsWith(".dynamics.com", StringComparison.OrdinalIgnoreCase) == true;
        }

        internal static IEnumerable<NodeInfo> GetTableNodes(IAttributeMetadataCache metadata, string nodePath, bool archive)
        {
            var entities = metadata.GetAllEntities();
            if (archive)
                entities = entities.Where(entity => entity.IsArchivalEnabled == true || entity.IsRetentionEnabled == true);

            return entities.OrderBy(entity => entity.LogicalName).Select(entity => new NodeInfo
            {
                IsLeaf = false,
                Label = entity.LogicalName,
                NodePath = nodePath + "/" + entity.LogicalName,
                NodeType = "Table",
                Metadata = new ObjectMetadata
                {
                    Urn = nodePath + "/" + entity.LogicalName,
                    MetadataType = MetadataType.Table,
                    Schema = archive ? "archive" : "dbo",
                    Name = entity.LogicalName
                }
            });
        }

        internal static IEnumerable<NodeInfo> GetColumnNodes(IAttributeMetadataCache metadata, string nodePath)
        {
            var tableName = new Uri(nodePath).AbsolutePath.Split('/')[2];
            var entity = metadata[tableName];
            foreach (var attribute in entity.Attributes.Where(attribute => attribute.AttributeOf == null).OrderBy(attribute => attribute.LogicalName))
            {
                yield return Column(attribute.LogicalName);
                if (attribute.AttributeType == AttributeTypeCode.Boolean ||
                    attribute.AttributeType == AttributeTypeCode.Picklist ||
                    attribute.AttributeType == AttributeTypeCode.State ||
                    attribute.AttributeType == AttributeTypeCode.Status ||
                    attribute.AttributeType == AttributeTypeCode.Lookup ||
                    attribute.AttributeType == AttributeTypeCode.Owner ||
                    attribute.AttributeType == AttributeTypeCode.Customer)
                    yield return Column(attribute.LogicalName + "name");

                if (attribute is LookupAttributeMetadata lookup && lookup.Targets?.Length > 1)
                    yield return Column(attribute.LogicalName + "type");
            }

            NodeInfo Column(string name) => new NodeInfo
            {
                IsLeaf = true,
                Label = name,
                NodePath = nodePath + "/" + name,
                NodeType = "Column",
                Metadata = new ObjectMetadata
                {
                    Urn = nodePath + "/" + name,
                    Name = tableName + "." + name
                }
            };
        }

        private bool HasEntity(DataSourceWithInfo dataSource, string logicalName)
        {
            try
            {
                return dataSource.Metadata[logicalName] != null;
            }
            catch
            {
                return false;
            }
        }

        private bool HandleRefresh(RefreshParams args)
        {
            return HandleExpand(args);
        }
    }
}
