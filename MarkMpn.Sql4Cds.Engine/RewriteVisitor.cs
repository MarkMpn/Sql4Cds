using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    class RewriteVisitor : RewriteVisitorBase
    {
        private readonly IDictionary<string, string> _mappings;

        public RewriteVisitor(IDictionary<ScalarExpression,string> rewrites)
        {
            _mappings = rewrites.ToDictionary(kvp => Serialize(kvp.Key), kvp => kvp.Value);
        }

        private string Serialize(ScalarExpression expr)
        {
            new Sql150ScriptGenerator().GenerateScript(expr, out var sql);
            return sql;
        }

        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

            if (expression == null)
                return null;

            if (_mappings.TryGetValue(Serialize(expression), out var column))
            {
                name = column;
                return new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier
                            {
                                Value = column
                            }
                        }
                    }
                };
            }

            return expression;
        }
    }
}
