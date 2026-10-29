using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Exposes common properties that are normally accessed via the <see cref="Microsoft.Xrm.Tooling.Connector.CrmServiceClient"/> or
    /// <see cref="Microsoft.PowerPlatform.Dataverse.Client.ServiceClient"/> classes, but are not available on the <see cref="IOrganizationService"/> interface.
    /// This allows other external implementations to expose the information in more efficient ways than the default implementation.
    /// </summary>
    public interface IServiceClient
    {
        /// <summary>
        /// The server version of the connected instance.
        /// </summary>
        Version Version { get; }

        /// <summary>
        /// The name of the connected organisation.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The URI of the connected organisation.
        /// </summary>
        Uri Uri { get; }

        /// <summary>
        /// Whether cloned connections can execute requests concurrently.
        /// </summary>
        bool CanParallelise { get; }

        /// <summary>
        /// Whether requests are translated to the Dataverse Web API.
        /// </summary>
        bool UsesWebAPI { get; }

        /// <summary>
        /// The system user to impersonate, or <see cref="Guid.Empty"/> to use the authenticated user.
        /// </summary>
        Guid CallerId { get; set; }

        /// <summary>
        /// The server's recommended maximum number of concurrent requests.
        /// </summary>
        int RecommendedDegreesOfParallelism { get; }

        /// <summary>
        /// Creates a connection for a parallel worker. Thread-safe caches and transports may be shared.
        /// </summary>
        IOrganizationService Clone();
    }

    static class ServiceClientExtensions
    {
        public static IOrganizationService CloneConnection(this IOrganizationService org)
        {
#if NETCOREAPP
            if (org is ServiceClient svc)
                return svc.Clone();
#else
            if (org is CrmServiceClient svc)
                return svc.Clone();
#endif

            if (org is IServiceClient sc)
                return sc.Clone();

            throw new NotSupportedException("The connection does not support cloning for parallel execution");
        }

        public static Version GetConnectedOrgVersion(this IOrganizationService org)
        {
#if NETCOREAPP
            if (org is ServiceClient svc)
                return svc.ConnectedOrgVersion;
#else
            if (org is CrmServiceClient svc)
                return svc.ConnectedOrgVersion;
#endif

            if (org is IServiceClient sc && sc.Version != null)
                return sc.Version;

            return new Version(((RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest())).Version);
        }
    }
}
