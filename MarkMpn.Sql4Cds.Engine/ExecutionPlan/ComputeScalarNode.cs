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
    /// Calculates the value of scalar expressions
    /// </summary>
    public class ComputeScalarNode : BaseNode
    {
        /// <summary>
        /// The names and associated expressions for the columns to calculate
        /// </summary>
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        /// <summary>
        /// The data source to use for the calculations
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata);

            foreach (var entity in Source.Execute(org, metadata, options, parameterValues))
            {
                foreach (var col in Columns)
                    entity[col.Key] = col.Value.GetValue(entity, schema);

                yield return entity;
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return Columns.Values
                .SelectMany(expr => expr.GetColumns())
                .Distinct();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            // Copy the source schema and add in the additional computed columns
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
