using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds
{
    public class NotSupportedQueryFragmentException : NotSupportedException
    {
        public NotSupportedQueryFragmentException(string message, TSqlFragment fragment) : base(message + ": " + GetText(fragment))
        {
            Error = message;
            Fragment = fragment;
        }

        public string Error { get; set; }
        public TSqlFragment Fragment { get; set; }
        private static string GetText(TSqlFragment fragment)
        {
            return String.Join("",
                fragment.ScriptTokenStream
                    .Skip(fragment.FirstTokenIndex)
                    .Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1)
                    .Select(t => t.Text));
        }
    }
}
