using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Wmhelp.XPath2;
using Wmhelp.XPath2.AST;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class XPath2ExpressionContext : NodeProvider
    {
        private static readonly XmlNamespaceManager _xmlNamespaceManager;
        public const string SqlNamespace = "https://markcarrington.dev/sql-4-cds";

        static XPath2ExpressionContext()
        {
            _xmlNamespaceManager = new XmlNamespaceManager(new NameTable());
            _xmlNamespaceManager.AddNamespace("sql", SqlNamespace);

            FunctionTable.Inst.Add(SqlNamespace, "column", XPath2ResultType.Any, (_, ctx, args) =>
            {
                var xpathContext = (XPath2ExpressionContext)ctx;
                var colName = (string)args[0];
                if (!xpathContext.Schema.ContainsColumn(colName, out var normalizedColName))
                    throw new QueryExecutionException(Sql4CdsError.InvalidColumnName(colName));

                var value = (INullable)xpathContext.ExpressionExecutionContext.Entity[normalizedColName];
                return SqlTypeConverter.SqlToNetType(value);
            });

            FunctionTable.Inst.Add(SqlNamespace, "variable", XPath2ResultType.Any, (_, ctx, args) =>
            {
                var xpathContext = (XPath2ExpressionContext)ctx;
                var varName = (string)args[0];
                if (!xpathContext.ExpressionExecutionContext.ParameterValues.TryGetValue(varName, out var value))
                    throw new QueryExecutionException(Sql4CdsError.XQueryMissingVariable(varName));
                return SqlTypeConverter.SqlToNetType(value);
            });
        }

        public static XPath2Expression Compile(string query)
        {
            return XPath2Expression.Compile(query, _xmlNamespaceManager);
        }

        public XPath2ExpressionContext(ExpressionExecutionContext context, INodeSchema schema, XPathItem item) : base(item)
        {
            ExpressionExecutionContext = context;
            Schema = schema;
        }

        public ExpressionExecutionContext ExpressionExecutionContext { get; }

        public INodeSchema Schema { get; }
    }

    abstract class XPathVisitor
    {
        public void Visit(AbstractNode node)
        {
            if (node is FuncNode func)
                Visit(func);

            if (node.Count == 0)
                return;

            foreach (var child in node)
                Visit(child);
        }

        private void Visit(FuncNode func)
        {
            // We want to get the name of the function but it's not exposed, get it with reflection
            var name = func.GetName();
            var ns = func.GetNS();

            Visit(func, name, ns);
        }

        protected abstract void Visit(FuncNode func, string name, string ns);
    }

    static class XPathExtensions
    {
        public static void Accept(this AbstractNode node, XPathVisitor visitor)
        {
            visitor.Visit(node);
        }

        public static string GetName(this FuncNode node)
        {
            return (string)typeof(FuncNode).GetField("_name", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(node);
        }

        public static string GetNS(this FuncNode node)
        {
            return (string)typeof(FuncNode).GetField("_ns", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(node);
        }
    }

    class XPathColumnCollectingVisitor : XPathVisitor
    {
        private readonly FunctionCall _func;

        public XPathColumnCollectingVisitor(FunctionCall func)
        {
            _func = func;
        }

        public List<string> Columns { get; } = new List<string>();

        protected override void Visit(FuncNode func, string name, string ns)
        {
            if (name == "column" && ns == XPath2ExpressionContext.SqlNamespace)
            {
                if (func.Count < 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.XQueryNotEnoughArguments(_func, name));

                if (func.Count > 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.XQueryTooManyArguments(_func, name));

                if (!(func[0] is ValueNode arg) ||
                    !(arg.Content is string col))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.XQueryStringLiteralRequired(_func));

                Columns.Add(col);
            }
        }
    }
}
