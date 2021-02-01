using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class HashMatchAggregateNode : BaseNode
    {
        public List<ColumnReferenceExpression> GroupBy { get; } = new List<ColumnReferenceExpression>();

        public Dictionary<string, Aggregate> Aggregates { get; } = new Dictionary<string, Aggregate>();

        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            foreach (var group in GroupBy)
            {
                foreach (var col in group.GetColumns())
                    yield return col;
            }

            foreach (var aggregate in Aggregates.Values)
            {
                if (aggregate.Expression == null)
                    continue;

                foreach (var col in aggregate.Expression.GetColumns())
                    yield return col;
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            var sourceSchema = Source.GetSchema(metadata);
            var schema = new NodeSchema();

            foreach (var group in GroupBy)
            {
                var colName = group.GetColumnName();
                sourceSchema.ContainsColumn(colName, out var normalized);
                schema.Schema[normalized] = sourceSchema.Schema[normalized];

                foreach (var alias in sourceSchema.Aliases.Where(a => a.Value.Contains(normalized)))
                {
                    if (!schema.Aliases.TryGetValue(alias.Key, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[alias.Key] = aliases;
                    }

                    aliases.Add(normalized);
                }
            }

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }
    }
}
