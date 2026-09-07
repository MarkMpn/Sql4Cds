using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    class ConnectionHandler : IJsonRpcMethodHandler
    {
        private readonly JsonRpc _lsp;
        private readonly ConnectionManager _connectionManager;
        private readonly object _attemptLock = new object();
        private readonly Dictionary<string, ConnectParams> _attempts = new Dictionary<string, ConnectParams>();

        public ConnectionHandler(JsonRpc lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(ConnectionRequest.Type, HandleConnection);
            lsp.AddHandler(CancelConnectRequest.Type, HandleCancelConnect);
            lsp.AddHandler(DisconnectRequest.Type, HandleDisconnect);
        }

        public bool HandleConnection(ConnectParams request)
        {
            lock (_attemptLock) _attempts[request.OwnerUri] = request;
            _ = Task.Run(() =>
            {
                var pendingOwnerUri = "sql4cds-connect://" + Guid.NewGuid().ToString("N");
                try
                {
                    // Authenticate on a temporary owner. A superseded or cancelled attempt
                    // must never change the editor's currently attached environment.
                    var session = _connectionManager.Connect(request.Connection, pendingOwnerUri);
                    lock (_attemptLock)
                    {
                        if (!_attempts.TryGetValue(request.OwnerUri, out var current) || !ReferenceEquals(current, request))
                            return;
                        session = _connectionManager.AssociateConnection(pendingOwnerUri, request.OwnerUri);
                        _attempts.Remove(request.OwnerUri);

                        _ = _lsp.NotifyWithParameterObjectAsync("connection/complete", new ConnectionCompleteParams
                        {
                            OwnerUri = request.OwnerUri,
                            RequestId = request.RequestId,
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
                    }
                }
                catch (Exception ex)
                {
                    _ = _lsp.NotifyAsync(ConnectionCompleteNotification.Type, new ConnectionCompleteParams
                    {
                        OwnerUri = request.OwnerUri,
                        RequestId = request.RequestId,
                        Type = request.Type,
                        Messages = ex.Message,
                        ErrorMessage = ex.Message
                    });
                }
                finally
                {
                    lock (_attemptLock)
                    {
                        if (_attempts.TryGetValue(request.OwnerUri, out var current) && ReferenceEquals(current, request))
                            _attempts.Remove(request.OwnerUri);
                        _connectionManager.Disconnect(pendingOwnerUri);
                    }
                }
            });

            return true;
        }

        public bool HandleCancelConnect(CancelConnectParams request)
        {
            lock (_attemptLock)
            {
                if (_attempts.TryGetValue(request.OwnerUri, out var current) &&
                    (request.RequestId == null || request.RequestId == current.RequestId))
                    _attempts.Remove(request.OwnerUri);
            }
            return true;
        }

        public bool HandleDisconnect(DisconnectParams request)
        {
            lock (_attemptLock)
            {
                _attempts.Remove(request.OwnerUri);
                _connectionManager.Disconnect(request.OwnerUri);
            }
            return true;
        }
    }
}
