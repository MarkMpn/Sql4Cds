using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    public static class TSqlFragmentExtensions
    {
        /// <summary>
        /// Converts a <see cref="TSqlFragment"/> to the corresponding SQL string
        /// </summary>
        /// <param name="fragment">The SQL DOM fragment to convert</param>
        /// <returns>The SQL string that the fragment can be parsed from</returns>
        public static string ToSql(this TSqlFragment fragment)
        {
            if (fragment.ScriptTokenStream != null)
            {
                return String.Join("",
                    fragment.ScriptTokenStream
                        .Skip(fragment.FirstTokenIndex)
                        .Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1)
                        .Select(t => t.Text));
            }

            new Sql150ScriptGenerator().GenerateScript(fragment, out var sql);
            return sql;
        }
    }
}
