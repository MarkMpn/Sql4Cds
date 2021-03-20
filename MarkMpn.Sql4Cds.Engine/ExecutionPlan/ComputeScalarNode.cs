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
    public class ComputeScalarNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The names and associated expressions for the columns to calculate
        /// </summary>
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        /// <summary>
        /// The data source to use for the calculations
        /// </summary>
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata, parameterTypes);

            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                foreach (var col in Columns)
                    entity[col.Key] = col.Value.GetValue(entity, schema, parameterTypes, parameterValues);

                yield return entity;
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            // Copy the source schema and add in the additional computed columns
            var sourceSchema = Source.GetSchema(metadata, parameterTypes);
            var schema = new NodeSchema { PrimaryKey = sourceSchema.PrimaryKey };

            foreach (var col in sourceSchema.Schema)
                schema.Schema.Add(col);

            foreach (var alias in sourceSchema.Aliases)
                schema.Aliases.Add(alias);

            foreach (var calc in Columns)
                schema.Schema[calc.Key] = calc.Value.GetType(sourceSchema, parameterTypes);

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var calcSourceColumns = Columns.Values
                   .SelectMany(expr => expr.GetColumns());

            foreach (var col in calcSourceColumns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize);
        }
    }
}
