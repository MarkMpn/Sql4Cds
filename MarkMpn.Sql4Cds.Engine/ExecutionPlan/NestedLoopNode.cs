﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a nested loop join
    /// </summary>
    public class NestedLoopNode : BaseJoinNode
    {
        /// <summary>
        /// The condition that must be true for  two records to join
        /// </summary>
        public BooleanExpression JoinCondition { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var leftSchema = LeftSource.GetSchema(metadata);
            var rightSchema = RightSource.GetSchema(metadata);
            var mergedSchema = GetSchema(metadata);

            foreach (var left in LeftSource.Execute(org, metadata, options))
            {
                foreach (var right in RightSource.Execute(org, metadata, options))
                {
                    var merged = Merge(left, leftSchema, right, rightSchema);

                    if (JoinCondition == null || JoinCondition.GetValue(merged, mergedSchema))
                        yield return merged;
                }
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return JoinCondition.GetColumns();
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            return this;
        }
    }
}