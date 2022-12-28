using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities
{
    class CapabilitiesHandler : IJsonRpcMethodHandler
    {
        private JsonRpc _lsp;
        private readonly VersionChecker _versionChecker;

        public CapabilitiesHandler(VersionChecker versionChecker)
        {
            _versionChecker = versionChecker;
        }

        public void Initialize(JsonRpc lsp)
        {
            _lsp = lsp;

            lsp.AddHandler(Methods.Initialize, HandleInitialize);
            lsp.AddHandler(CapabilitiesRequest.Type, HandleCapabilities);
        }

        private InitializeResult HandleInitialize(InitializeParams arg)
        {
            return new InitializeResult
            {
                Capabilities = new ServerCapabilities
                {
                    CompletionProvider = new CompletionOptions
                    {
                        WorkDoneProgress = false
                    },
                    HoverProvider = true,
                    SignatureHelpProvider = new SignatureHelpOptions
                    {
                        WorkDoneProgress = false
                    },
                    TextDocumentSync = new TextDocumentSyncOptions
                    {
                        Change = TextDocumentSyncKind.Full,
                        OpenClose = true
                    }
                }
            };
        }

        private const string Banner =
        @"
  ___  ___  _      _ _     ___ ___  ___ 
 / __|/ _ \| |    | | |   / __|   \/ __|
 \__ \ (_) | |__  |_  _| | (__| |) \__ \
 |___/\__\_\____|   |_|   \___|___/|___/  v{version}

 SQL commands are implemented by SQL 4 CDS
 and not supported by Microsoft
 https://markcarrington.dev/sql-4-cds/

";

        public CapabilitiesResult HandleCapabilities(CapabilitiesRequest request)
        {
            var currentVersion = Assembly.GetExecutingAssembly().GetName().Version;
            _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
            {
                MessageType = MessageType.Log,
                Message = Banner.Replace("{version}", currentVersion.ToString(3))
            });

            _ = _versionChecker.Result.ContinueWith(result =>
            {
                if (result.IsCompleted && !result.IsFaulted && result.Result > currentVersion)
                {
                    _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
                    {
                        MessageType = MessageType.Warning,
                        Message = "An updated version of SQL 4 CDS is available"
                    });
                    _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
                    {
                        MessageType = MessageType.Warning,
                        Message = $"Update to v{result.Result.ToString(3)} available from https://markcarrington.dev/sql-4-cds/"
                    });
                }
            }, TaskScheduler.Default);

            return new CapabilitiesResult
            {
                Capabilities = new DmpServerCapabilities
                {
                    ProtocolVersion = "1.0",
                    ProviderName = "SQL4CDS",
                    ProviderDisplayName = "SQL 4 CDS",
                    ConnectionProvider = new ConnectionProviderOptions
                    {
                        Options = new[]
                        {
                            new ConnectionOption
                            {
                                SpecialValueType = ConnectionOption.SpecialValueServerName,
                                IsIdentity = true
                            },
                            new ConnectionOption
                            {
                                SpecialValueType = ConnectionOption.SpecialValueDatabaseName,
                                IsIdentity = true
                            },
                            // TODO: More?
                        }
                    },
                    AdminServicesProvider = new AdminServicesProviderOptions
                    {
                        DatabaseInfoOptions = new[]
                        {
                            new ServiceOption
                            {
                                Name = "name",
                                DisplayName = "Name",
                                Description = "Name of the database",
                                ValueType = "string",
                                IsRequired = true,
                                GroupName = "General"
                            },
                            new ServiceOption
                            {
                                Name = "url",
                                DisplayName = "Url",
                                Description = "Url of the database",
                                ValueType = "string",
                                IsRequired = true,
                                GroupName = "General"
                            }
                        }
                    },
                    Features = new[]
                    {
                        new FeatureMetadataProvider
                        {
                            FeatureName = "serializationService",
                            Enabled = true,
                            OptionsMetadata = Array.Empty<ServiceOption>()
                        }
                    }
                }
            };
        }
    }
}
