using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    static class MetadataQueryExtensions
    {
        public static void AddFilter(this MetadataQueryExpression query, MetadataFilterExpression filter)
        {
            if (filter == null)
                return;
            
            if (query.Criteria == null || query.Criteria.Conditions.Count == 0 && query.Criteria.Filters.Count == 0)
                query.Criteria = filter;
            else if (query.Criteria.FilterOperator == LogicalOperator.And)
                query.Criteria.Filters.Add(filter);
            else
                query.Criteria = new MetadataFilterExpression { Filters = { query.Criteria, filter } };
        }
    }
}
