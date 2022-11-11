using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Models;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    [Method("capabilities/list", Direction.ClientToServer)]
    [Serial]
    class CapabilitiesHandler : IRequestHandler<CapabilitiesRequest, CapabilitiesResult>, IJsonRpcHandler
    {
        public Task<CapabilitiesResult> Handle(CapabilitiesRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new CapabilitiesResult
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
                            // TODO: More
                        }
                    },
                    AdminServicesProvider = new AdminServicesProviderOptions
                    {
                    },
                    Features = new []
                    {
                        new FeatureMetadataProvider
                        {
                            FeatureName = "serializationService",
                            Enabled = true,
                            OptionsMetadata = Array.Empty<ServiceOption>()
                        }
                    }
                }
            });
        }
    }

    [Method("capabilities/list", Direction.ClientToServer)]
    [Serial]
    class CapabilitiesRequest : IRequest<CapabilitiesResult>
    {

    }

    class CapabilitiesResult
    {
        public DmpServerCapabilities Capabilities { get; set; }
    }

    /// <summary>
    /// Defines the DMP server capabilities
    /// </summary>
    public class DmpServerCapabilities
    {
        public string ProtocolVersion { get; set; }

        public string ProviderName { get; set; }

        public string ProviderDisplayName { get; set; }

        public ConnectionProviderOptions ConnectionProvider { get; set; }

        public AdminServicesProviderOptions AdminServicesProvider { get; set; }

        /// <summary>
        /// List of features
        /// </summary>
        public FeatureMetadataProvider[] Features { get; set; }
    }

    public class ConnectionProviderOptions
    {
        public ConnectionOption[] Options { get; set; }
    }

    public class ConnectionOption : ServiceOption
    {
        public static readonly string SpecialValueServerName = "serverName";
        public static readonly string SpecialValueDatabaseName = "databaseName";
        public static readonly string SpecialValueAuthType = "authType";
        public static readonly string SpecialValueUserName = "userName";
        public static readonly string SpecialValuePasswordName = "password";
        public static readonly string SpecialValueAppName = "appName";

        /// <summary>
        /// Determines if the parameter is one of the 'special' known values.
        /// Can be either Server Name, Database Name, Authentication Type,
        /// User Name, or Password
        /// </summary>
        public string SpecialValueType { get; set; }

        /// <summary>
        /// Flag to indicate that this option is part of the connection identity
        /// </summary>
        public bool IsIdentity { get; set; }
    }
    /// <summary>
    /// Defines the admin services provider options that the DMP server implements. 
    /// </summary>
    public class AdminServicesProviderOptions
    {
        public ServiceOption[] DatabaseInfoOptions { get; set; }

        public ServiceOption[] DatabaseFileInfoOptions { get; set; }

        public ServiceOption[] FileGroupInfoOptions { get; set; }
    }
    public class ServiceOption
    {
        public static readonly string ValueTypeString = "string";
        public static readonly string ValueTypeMultiString = "multistring";
        public static readonly string ValueTypePassword = "password";
        public static readonly string ValueTypeNumber = "number";
        public static readonly string ValueTypeCategory = "category";
        public static readonly string ValueTypeBoolean = "boolean";
        public static readonly string ValueTypeObject = "object";

        public string Name { get; set; }

        public string DisplayName { get; set; }

        public string Description { get; set; }

        public string GroupName { get; set; }

        /// <summary>
        /// Type of the parameter.  Can be either string, number, or category.
        /// </summary>
        public string ValueType { get; set; }

        public string DefaultValue { get; set; }

        public string ObjectType { get; set; }

        /// <summary>
        /// Set of permitted values if ValueType is category.
        /// </summary>
        public CategoryValue[] CategoryValues { get; set; }

        /// <summary>
        /// Flag to indicate that this option is required
        /// </summary>
        public bool IsRequired { get; set; }

        public bool IsArray { get; set; }
    }
    /// <summary>
    /// Includes the metadata for a feature
    /// </summary>
    public class FeatureMetadataProvider
    {
        /// <summary>
        /// Indicates whether the feature is enabled 
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Feature name
        /// </summary>
        public string FeatureName { get; set; }

        /// <summary>
        /// The options metadata avaialble for this feature
        /// </summary>
        public ServiceOption[] OptionsMetadata { get; set; }

    }

    public class CategoryValue
    {
        public string DisplayName { get; set; }

        public string Name { get; set; }
    }

}
