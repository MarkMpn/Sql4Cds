using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Sorts the data in the data stream
    /// </summary>
    class SortNode : BaseNode
    {
        /// <summary>
        /// The sorts to apply
        /// </summary>
        public List<ExpressionWithSortOrder> Sorts { get; } = new List<ExpressionWithSortOrder>();

        /// <summary>
        /// The data source to sort
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var source = Source.Execute(org, metadata, options);
            IOrderedEnumerable<Entity> sortedSource;

            if (Sorts[0].SortOrder == SortOrder.Descending)
                sortedSource = source.OrderBy(e => Sorts[0].Expression.GetValue(e));
            else
                sortedSource = source.OrderByDescending(e => Sorts[0].Expression.GetValue(e));

            for (var i = 1; i < Sorts.Count; i++)
            {
                if (Sorts[i].SortOrder == SortOrder.Descending)
                    sortedSource = sortedSource.ThenBy(e => Sorts[i].Expression.GetValue(e));
                else
                    sortedSource = sortedSource.ThenByDescending(e => Sorts[i].Expression.GetValue(e));
            }

            return sortedSource;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return Source.GetSchema(metadata);
        }
    }
}
