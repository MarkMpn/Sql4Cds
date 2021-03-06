﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a TOP clause
    /// </summary>
    class TopNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The number of records to retrieve
        /// </summary>
        [Category("Top")]
        [Description("The number of records to retrieve")]
        public ScalarExpression Top { get; set; }

        /// <summary>
        /// Indicates if the Top number indicates a percentage or an absolute number of records
        /// </summary>
        [Category("Top")]
        [Description("Indicates if the Top number indicates a percentage or an absolute number of records")]
        public bool Percent { get; set; }

        /// <summary>
        /// Indicates if two records with the same sort order should be included even if the total number of records has been met
        /// </summary>
        [Category("Top")]
        [Description("Indicates if two records with the same sort order should be included even if the total number of records has been met")]
        public bool WithTies { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (WithTies)
                throw new NotImplementedException();

            var topCount = Top.Compile(null, parameterTypes)(null, parameterValues);

            if (!Percent)
            {
                return Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                    .Take(SqlTypeConverter.ChangeType<int>(topCount));
            }
            else
            {
                var count = Source.Execute(org, metadata, options, parameterTypes, parameterValues).Count();
                var top = count * SqlTypeConverter.ChangeType<float>(topCount) / 100;

                return Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                    .Take((int)top);
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            if (!Top.IsConstantValueExpression(null, out var literal))
                return this;

            // FetchXML can support TOP directly provided it's for no more than 5,000 records
            if (!Percent && !WithTies && Int32.TryParse(literal.Value, out var top) && top <= 5000)
            {
                var fetchXml = Source as FetchXmlScan;

                // Skip over ComputeScalar nodes to fold the TOP into the previous FetchXML node
                if (fetchXml == null && Source is ComputeScalarNode computeScalar)
                    fetchXml = computeScalar.Source as FetchXmlScan;

                if (fetchXml != null)
                {
                    fetchXml.FetchXml.top = literal.Value;
                    fetchXml.AllPages = false;

                    if (Source == fetchXml)
                        return fetchXml;

                    return Source;
                }
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            var sourceCount = Source.EstimateRowsOut(metadata, parameterTypes, tableSize);

            if (!Top.IsConstantValueExpression(null, out var topLiteral))
                return sourceCount;

            var top = Int32.Parse(topLiteral.Value);

            return Math.Max(0, Math.Min(top, sourceCount));
        }
    }
}
