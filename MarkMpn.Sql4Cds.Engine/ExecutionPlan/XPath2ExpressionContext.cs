using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using System.Xml;
using System.Xml.XPath;
using Wmhelp.XPath2;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class XPath2ExpressionContext : NodeProvider
    {
        private static readonly XmlNamespaceManager _xmlNamespaceManager;
        private const string SqlNamespace = "https://markcarrington.dev/sql-4-cds";

        static XPath2ExpressionContext()
        {
            _xmlNamespaceManager = new XmlNamespaceManager(new NameTable());
            _xmlNamespaceManager.AddNamespace("sql", SqlNamespace);

            FunctionTable.Inst.Add(SqlNamespace, "column", XPath2ResultType.Any, (_, ctx, args) =>
            {
                var xpathContext = (XPath2ExpressionContext)ctx;
                var colName = (string)args[0];
                if (!xpathContext.Schema.ContainsColumn(colName, out var normalizedColName))
                    throw new QueryExecutionException("Missing column " + colName);

                var value = (INullable)xpathContext.ExpressionExecutionContext.Entity[normalizedColName];
                return SqlTypeConverter.SqlToNetType(value);
            });

            FunctionTable.Inst.Add(SqlNamespace, "variable", XPath2ResultType.Any, (_, ctx, args) =>
            {
                var xpathContext = (XPath2ExpressionContext)ctx;
                var varName = (string)args[0];
                if (!xpathContext.ExpressionExecutionContext.ParameterValues.TryGetValue(varName, out var value))
                    throw new QueryExecutionException("Missing variable " + varName);
                return value;
            });
        }

        public static XmlNamespaceManager XmlNamespaceManager => _xmlNamespaceManager;

        public XPath2ExpressionContext(ExpressionExecutionContext context, INodeSchema schema, XPathItem item) : base(item)
        {
            ExpressionExecutionContext = context;
            Schema = schema;
        }

        public ExpressionExecutionContext ExpressionExecutionContext { get; }

        public INodeSchema Schema { get; }
    }
}
