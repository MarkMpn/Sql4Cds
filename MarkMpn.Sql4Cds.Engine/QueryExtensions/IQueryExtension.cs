using Microsoft.Xrm.Sdk;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Provides post-processing of the query results to perform queries that aren't supported directly in FetchXML
    /// </summary>
    public interface IQueryExtension
    {
        /// <summary>
        /// Transforms the query results
        /// </summary>
        /// <param name="source">The query results to transform</param>
        /// <param name="options">The query execution options to apply</param>
        /// <returns>The transformed result set</returns>
        IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options);
    }
}
