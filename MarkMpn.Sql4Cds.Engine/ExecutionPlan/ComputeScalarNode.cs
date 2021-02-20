using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class ComputeScalarNode : BaseNode
    {
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return Columns.Values
                .SelectMany(expr => expr.GetColumns())
                .Distinct();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            var sourceSchema = Source.GetSchema(metadata);
            var schema = new NodeSchema { PrimaryKey = sourceSchema.PrimaryKey };

            foreach (var col in sourceSchema.Schema)
                schema.Schema.Add(col);

            foreach (var alias in sourceSchema.Aliases)
                schema.Aliases.Add(alias);

            foreach (var calc in Columns)
                schema.Schema[calc.Key] = calc.Value.GetType(sourceSchema);

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source = Source.MergeNodeDown(metadata, options);
            return this;
        }
    }
}
