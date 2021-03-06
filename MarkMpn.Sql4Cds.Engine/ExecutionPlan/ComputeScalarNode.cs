﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Calculates the value of scalar expressions
    /// </summary>
    class ComputeScalarNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The names and associated expressions for the columns to calculate
        /// </summary>
        [Category("Compute Scalar")]
        [Description("The names and associated expressions for the columns to calculate")]
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        /// <summary>
        /// The data source to use for the calculations
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata, parameterTypes);
            var columns = Columns
                .Select(kvp => new { Name = kvp.Key, Expression = kvp.Value.Compile(schema, parameterTypes) })
                .ToList();

            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                foreach (var col in columns)
                    entity[col.Name] = col.Expression(entity, parameterValues);

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
                schema.Schema[calc.Key] = calc.Value.GetType(sourceSchema, null, parameterTypes);

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);

            // Combine multiple ComputeScalar nodes. Calculations in this node might be dependent on those in the previous node, so rewrite any references
            // to the earlier computed columns
            if (Source is ComputeScalarNode computeScalar)
            {
                var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();

                foreach (var prevCalc in computeScalar.Columns)
                    rewrites[prevCalc.Key.ToColumnReference()] = prevCalc.Value;

                var rewrite = new RewriteVisitor(rewrites);

                foreach (var calc in Columns)
                    computeScalar.Columns.Add(calc.Key, rewrite.ReplaceExpression(calc.Value));

                return computeScalar;
            }

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
