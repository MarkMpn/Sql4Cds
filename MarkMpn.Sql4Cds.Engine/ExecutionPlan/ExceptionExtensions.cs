using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class ExceptionExtensions
    {
        public static bool IsThrottlingException(this FaultException<OrganizationServiceFault> ex, out TimeSpan retryDelay)
        {
            if (ex == null)
            {
                retryDelay = TimeSpan.Zero;
                return false;
            }

            if (ex.Detail.ErrorCode == 429 || // Virtual/elastic tables
                ex.Detail.ErrorCode == -2147015902 || // Number of requests exceeded the limit of 6000 over time window of 300 seconds.
                ex.Detail.ErrorCode == -2147015903 || // Combined execution time of incoming requests exceeded limit of 1,200,000 milliseconds over time window of 300 seconds. Decrease number of concurrent requests or reduce the duration of requests and try again later.
                ex.Detail.ErrorCode == -2147015898) // Number of concurrent requests exceeded the limit of 52.
            {
                retryDelay = TimeSpan.FromSeconds(2);

                if (ex.Detail.ErrorDetails.TryGetValue("Retry-After", out var retryAfter) && retryAfter is TimeSpan ts)
                    retryDelay = ts;

                if (retryDelay > TimeSpan.FromMinutes(5))
                    retryDelay = TimeSpan.FromMinutes(5);

                return true;
            }

            retryDelay = TimeSpan.Zero;
            return false;
        }
    }
}
