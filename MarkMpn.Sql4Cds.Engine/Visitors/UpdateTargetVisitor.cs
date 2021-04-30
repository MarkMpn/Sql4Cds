using System;
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
        private readonly string _search;
        private bool _foundAlias;
        private bool _ambiguous;

        public UpdateTargetVisitor(string search)
        {
            _search = search;
        }

        public string TargetEntityName { get; private set; }

        public string TargetAliasName { get; private set; }

        public NamedTableReference Target { get; private set; }

        public bool Ambiguous => _ambiguous;

        public override void ExplicitVisit(NamedTableReference node)
        {
            base.ExplicitVisit(node);

            if (node.Alias != null && node.Alias.Value.Equals(_search, StringComparison.OrdinalIgnoreCase))
            {
                _ambiguous = _foundAlias;

                TargetEntityName = node.SchemaObject.BaseIdentifier.Value;
                TargetAliasName = node.Alias.Value;
                Target = node;
                _foundAlias = true;
            }

            if (!_foundAlias && node.SchemaObject.BaseIdentifier.Value.Equals(_search, StringComparison.OrdinalIgnoreCase))
            {
                if (!_foundAlias && !String.IsNullOrEmpty(TargetEntityName))
                    _ambiguous = true;

                TargetEntityName = _search;
                TargetAliasName = node.Alias?.Value ?? _search;
                Target = node;
            }
        }
    }
}
