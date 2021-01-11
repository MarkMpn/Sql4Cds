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

        public UpdateTargetVisitor(string search)
        {
            _search = search;
        }

        public string TargetEntityName { get; set; }

        public string TargetAliasName { get; set; }

        public override void ExplicitVisit(NamedTableReference node)
        {
            base.ExplicitVisit(node);

            if (node.Alias != null && node.Alias.Value.Equals(_search, StringComparison.OrdinalIgnoreCase))
            {
                TargetEntityName = node.SchemaObject.BaseIdentifier.Value;
                TargetAliasName = node.Alias.Value;
                _foundAlias = true;
            }

            if (!_foundAlias && node.SchemaObject.BaseIdentifier.Value.Equals(_search, StringComparison.OrdinalIgnoreCase))
            {
                TargetEntityName = _search;
                TargetAliasName = node.Alias?.Value ?? _search;
            }
        }
    }
}
