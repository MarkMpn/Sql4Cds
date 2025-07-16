﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class UpdateTargetVisitor : TSqlFragmentVisitor
    {
        private readonly SchemaObjectName _search;
        private bool _foundAlias;
        private bool _ambiguous;

        public UpdateTargetVisitor(SchemaObjectName search, string primaryDataSource)
        {
            _search = search;
            PrimaryDataSource = primaryDataSource;
        }

        public string PrimaryDataSource { get; private set; }

        public string TargetDataSource { get; private set; }

        public string TargetSchema { get; private set; }

        public string TargetEntityName { get; private set; }

        public string TargetAliasName { get; private set; }

        public NamedTableReference Target { get; private set; }

        public QueryDerivedTable TargetSubquery { get; private set; }

        public CommonTableExpression TargetCTE { get; private set; }

        public bool Ambiguous => _ambiguous;

        public override void ExplicitVisit(NamedTableReference node)
        {
            base.ExplicitVisit(node);

            if (node.Alias != null && String.IsNullOrEmpty(_search.DatabaseIdentifier?.Value) && node.Alias.Value.Equals(_search.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
            {
                _ambiguous = _foundAlias;

                TargetDataSource = node.SchemaObject.DatabaseIdentifier?.Value ?? PrimaryDataSource;
                TargetSchema = node.SchemaObject.SchemaIdentifier?.Value;
                TargetEntityName = node.SchemaObject.BaseIdentifier.Value;
                TargetAliasName = node.Alias.Value;
                Target = node;
                _foundAlias = true;
            }

            if (!_foundAlias &&
                (node.SchemaObject.DatabaseIdentifier?.Value ?? PrimaryDataSource).Equals(_search.DatabaseIdentifier?.Value ?? PrimaryDataSource, StringComparison.OrdinalIgnoreCase) &&
                node.SchemaObject.BaseIdentifier.Value.Equals(_search.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
            {
                if (!_foundAlias && !String.IsNullOrEmpty(TargetEntityName))
                    _ambiguous = true;

                TargetDataSource = node.SchemaObject.DatabaseIdentifier?.Value ?? PrimaryDataSource;
                TargetSchema = node.SchemaObject.SchemaIdentifier?.Value;
                TargetEntityName = node.SchemaObject.BaseIdentifier.Value;
                TargetAliasName = node.Alias?.Value ?? _search.BaseIdentifier.Value;
                Target = node;
            }
        }

        public override void ExplicitVisit(ScalarSubquery node)
        {
            // Do not recurse into subqueries
        }

        public override void ExplicitVisit(QueryDerivedTable node)
        {
            if (node.Alias != null && String.IsNullOrEmpty(_search.DatabaseIdentifier?.Value) && node.Alias.Value.Equals(_search.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
            {
                _ambiguous = _foundAlias;

                TargetDataSource = null;
                TargetSchema = null;
                TargetEntityName = null;
                TargetAliasName = node.Alias.Value;
                TargetSubquery = node;
                _foundAlias = true;
            }

            // Do not recurse into subqueries
        }

        public override void ExplicitVisit(CommonTableExpression node)
        {
            if (node.ExpressionName != null && String.IsNullOrEmpty(_search.DatabaseIdentifier?.Value) && node.ExpressionName.Value.Equals(_search.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
            {
                _ambiguous = _foundAlias;

                TargetDataSource = null;
                TargetSchema = null;
                TargetEntityName = null;
                TargetAliasName = node.ExpressionName.Value;
                TargetCTE = node;
                _foundAlias = true;
            }

            // Do not recurse into CTEs
        }
    }
}
