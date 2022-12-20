using System;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities
{
    class CapabilitiesHandler : IJsonRpcMethodHandler
    {
        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(CapabilitiesRequest.Type, HandleCapabilities);
        }

        public CapabilitiesResult HandleCapabilities(CapabilitiesRequest request)
        {
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
