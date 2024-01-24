using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class ParallelismHelper
    {
        /// <summary>
        /// Calculates the MaxDOP setting for a node
        /// </summary>
        /// <param name="dataSource">The data source the node is working with</param>
        /// <param name="context">The context the node is being compiled in</param>
        /// <param name="queryHints">Any query hints that apply</param>
        /// <returns>The MaxDOP setting for the node</returns>
        /// <exception cref="NotSupportedQueryFragmentException"></exception>
        public static int GetMaxDOP(DataSource dataSource, NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            var org = dataSource.Connection;
            var recommendedMaxDop = 1;

#if NETCOREAPP
            var svc = org as ServiceClient;

            if (svc != null)
                recommendedMaxDop = svc.RecommendedDegreesOfParallelism;
#else
            var svc = org as CrmServiceClient;

            if (svc != null)
                recommendedMaxDop = svc.RecommendedDegreesOfParallelism;
#endif

            var maxDopHint = (queryHints ?? Array.Empty<OptimizerHint>())
                .OfType<LiteralOptimizerHint>()
                .Where(hint => hint.HintKind == OptimizerHintKind.MaxDop)
                .FirstOrDefault();

            if (maxDopHint != null)
            {
                if (!(maxDopHint.Value is IntegerLiteral maxDop) || !Int32.TryParse(maxDop.Value, out var value) || value < 0)
                    throw new NotSupportedQueryFragmentException(new Sql4CdsError(15, 102, "MAXDOP requires a positive integer value, or 0 to use recommended value", maxDopHint));

                if (value > 0)
                    return value;

                return recommendedMaxDop;
            }

            if (context.Options.MaxDegreeOfParallelism > 0)
                return context.Options.MaxDegreeOfParallelism;

            return recommendedMaxDop;
        }

        /// <summary>
        /// Checks if the specified connection can be used in a parallel query
        /// </summary>
        /// <param name="org">The connection to use</param>
        /// <returns></returns>
        public static bool CanParallelise(IOrganizationService org)
        {
#if NETCOREAPP
            var svc = org as ServiceClient;

            if (svc == null || (svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.OAuth && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.Certificate && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.ExternalTokenManagement && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.ClientSecret))
                return false;
#else
            var svc = org as CrmServiceClient;

            if (svc == null || (svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.OAuth && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.Certificate && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.ExternalTokenManagement && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.ClientSecret))
                return false;
#endif

            return true;
        }
    }
}
