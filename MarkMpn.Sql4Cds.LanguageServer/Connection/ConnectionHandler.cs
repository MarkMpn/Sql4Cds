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
            _ = Task.Run(() =>
            {
                try
                {
                    var session = _connectionManager.Connect(request.Connection, request.OwnerUri);

                    _ = _lsp.NotifyWithParameterObjectAsync("connection/complete", new ConnectionCompleteParams
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
                }
                catch (Exception ex)
                {
                    _ = _lsp.NotifyAsync(ConnectionCompleteNotification.Type, new ConnectionCompleteParams
                    {
                        OwnerUri = request.OwnerUri,
                        Type = request.Type,
                        Messages = ex.Message,
                        ErrorMessage = ex.Message
                    });
                }
            });

            return true;
        }

        public bool HandleCancelConnect(CancelConnectParams request)
        {
            return true;
        }

        public bool HandleDisconnect(DisconnectParams request)
        {
            _connectionManager.Disconnect(request.OwnerUri);
            return true;
        }
    }
}
