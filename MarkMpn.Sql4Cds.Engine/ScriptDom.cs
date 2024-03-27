using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.UI.WebControls;
using System.Xml.Linq;
using Castle.DynamicProxy.Generators.Emitters.SimpleAST;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;

/// <summary>
/// Basic shims for ScriptDom classes required to build a SQL statement from FetchXml
/// if the full ScriptDom assembly is not available
/// </summary>
namespace Microsoft.SqlServer.TransactSql.ScriptDom
{
    class TSqlBatch
    {
        public List<Statement> Statements { get; } = new List<Statement>();

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            for (var i = 0; i < Statements.Count; i++)
            {
                if (i > 0)
                    buf.Append("\r\n");

                Statements[i].ToString(buf, indent);
            }
        }
    }

    abstract class Statement
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf, int indent);
    }

    class DeclareVariableStatement : Statement
    {
        public List<DeclareVariableElement> Declarations { get; } = new List<DeclareVariableElement>();

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append(' ', indent);

            buf.Append("DECLARE ");

            for (var i = 0; i < Declarations.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                Declarations[i].ToString(buf, indent);
            }

            buf.Append("\r\n");
        }
    }

    class DeclareVariableElement
    {
        public Identifier VariableName { get; set; }

        public SqlDataTypeReference DataType { get; set; }

        public ScalarExpression Value { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            VariableName.ToString(buf);
            buf.Append(" ");
            DataType.ToString(buf, indent);

            if (Value != null)
            {
                buf.Append(" = ");
                Value.ToString(buf, indent);
            }
        }
    }

    class SelectStatement : Statement
    {
        public QueryExpression QueryExpression { get; set; }

        public WithCtesAndXmlNamespaces WithCtesAndXmlNamespaces { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            WithCtesAndXmlNamespaces?.ToString(buf, indent);
            QueryExpression?.ToString(buf, indent);
        }
    }

    abstract class QueryExpression
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf, int indent);
    }

    class QuerySpecification : QueryExpression
    {
        public TopRowFilter TopRowFilter { get; set; }

        public UniqueRowFilter UniqueRowFilter { get; set; }

        public List<SelectElement> SelectElements { get; } = new List<SelectElement>();

        public FromClause FromClause { get; set; }

        public WhereClause WhereClause { get; set; }

        public GroupByClause GroupByClause { get; set; }

        public OrderByClause OrderByClause { get; set; }

        public OffsetClause OffsetClause { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append(' ', indent);

            var longestClause = "SELECT";
            if (GroupByClause != null)
                longestClause = "GROUP BY";
            else if (OrderByClause != null)
                longestClause = "ORDER BY";

            buf.Append("SELECT");

            buf.Append(' ', longestClause.Length - "SELECT".Length + 1);

            if (UniqueRowFilter == UniqueRowFilter.Distinct)
                buf.Append("DISTINCT ");

            TopRowFilter?.ToString(buf, indent);

            var selectIndent = Sql160ScriptGenerator.GetCurrentIndent(buf);

            for (var i = 0; i < SelectElements.Count; i++)
            {
                if (i > 0)
                {
                    buf.Append(",\r\n");
                    buf.Append(' ', selectIndent);
                }

                SelectElements[i].ToString(buf, selectIndent);
            }

            buf.Append("\r\n");

            FromClause?.ToString(buf, indent, longestClause.Length);
            WhereClause?.ToString(buf, indent, longestClause.Length);
            GroupByClause?.ToString(buf, indent, longestClause.Length);
            OrderByClause?.ToString(buf, indent, longestClause.Length);
            OffsetClause?.ToString(buf, indent, longestClause.Length);
        }
    }

    class BinaryQueryExpression : QueryExpression
    {
        public QueryExpression FirstQueryExpression { get; set; }

        public BinaryQueryExpressionType BinaryQueryExpressionType { get; set; }

        public bool All { get; set; }

        public QueryExpression SecondQueryExpression { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstQueryExpression.ToString(buf, indent);

            switch (BinaryQueryExpressionType)
            {
                case BinaryQueryExpressionType.Union:
                    buf.Append("\r\n");
                    buf.Append(' ', indent);
                    buf.Append("UNION");
                    break;

                default:
                    throw new NotSupportedException();
            }

            if (All)
                buf.Append(" ALL");

            buf.Append("\r\n");

            SecondQueryExpression.ToString(buf, indent);
        }
    }

    enum BinaryQueryExpressionType
    {
        Union
    }

    class TopRowFilter
    {
        public Expression Expression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            buf.Append("TOP ");
            Expression?.ToString(buf, indent);
            buf.Append(" ");
        }
    }

    abstract class Expression
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf, int indent);
    }

    abstract class Literal : ScalarExpression
    {
        public string Value { get; set; }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append(Value);
        }
    }

    class NullLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("NULL");
        }
    }

    class IntegerLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }
    }

    class StringLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("'");
            buf.Append(Value.Replace("'", "''"));
            buf.Append("'");
        }
    }

    class BinaryLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }
    }

    class NumericLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }
    }

    class MoneyLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }
    }

    class VariableReference : ScalarExpression
    {
        public string Name { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append(Name);
        }
    }

    enum UniqueRowFilter
    {
        None,
        Distinct
    }

    class FromClause
    {
        public List<TableReference> TableReferences { get; } = new List<TableReference>();

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent, int longestClauseLength)
        {
            buf.Append(' ', indent);
            buf.Append("FROM");
            buf.Append(' ', longestClauseLength - "FROM".Length + 1);

            for (var i = 0; i < TableReferences.Count; i++)
            {
                if (i > 0)
                {
                    buf.Append(",\r\n");
                    buf.Append(' ', indent + longestClauseLength + 1);
                }

                TableReferences[i].ToString(buf, indent + longestClauseLength + 1);
            }

            buf.Append("\r\n");
        }
    }

    abstract class TableReference
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf, int indent);
    }

    class NamedTableReference : TableReference
    {
        public SchemaObjectName SchemaObject { get; set; }

        public List<TableHint> TableHints { get; } = new List<TableHint>();

        public Identifier Alias { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            SchemaObject.ToString(buf, indent);

            if (Alias != null)
            {
                buf.Append(" AS ");
                Alias.ToString(buf);
            }

            if (TableHints.Count > 0)
            {
                buf.Append(" WITH (");

                for (var i = 0; i < TableHints.Count; i++)
                {
                    if (i > 0)
                        buf.Append(", ");

                    TableHints[i].ToString(buf);
                }

                buf.Append(") ");
            }
        }
    }

    class QualifiedJoin : TableReference
    {
        public TableReference FirstTableReference { get; set; }

        public TableReference SecondTableReference { get; set; }

        public QualifiedJoinType QualifiedJoinType { get; set; }

        public BooleanExpression SearchCondition { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstTableReference.ToString(buf, indent);

            buf.Append("\r\n");
            buf.Append(' ', indent);

            if (QualifiedJoinType == QualifiedJoinType.Inner)
                buf.Append("INNER JOIN");
            else
                buf.Append("LEFT OUTER JOIN");

            buf.Append("\r\n");
            buf.Append(' ', indent);

            SecondTableReference.ToString(buf, indent);

            buf.Append("\r\n");
            buf.Append(' ', indent);

            buf.Append("ON ");

            SearchCondition.ToString(buf, indent + 3);
        }
    }

    enum QualifiedJoinType
    {
        Inner,
        LeftOuter
    }

    class SchemaObjectName : MultiPartIdentifier
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }
    }

    class Identifier
    {
        public string Value { get; set; }

        public QuoteType QuoteType { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf)
        {
            if (QuoteType == QuoteType.SquareBracket)
                buf.Append("[");

            buf.Append(Value);

            if (QuoteType == QuoteType.SquareBracket)
                buf.Append("]");
        }
    }

    enum QuoteType
    {
        SquareBracket,
        NotQuoted
    }

    class MultiPartIdentifier
    {
        public List<Identifier> Identifiers { get; } = new List<Identifier>();

        public virtual void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            for (var i = 0; i < Identifiers.Count; i++)
            {
                if (i > 0)
                    buf.Append(".");

                Identifiers[i].ToString(buf);
            }
        }
    }

    class TableHint
    {
        public TableHintKind HintKind { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf)
        {
            switch (HintKind)
            {
                case TableHintKind.NoLock:
                    buf.Append("NOLOCK");
                    break;

                default:
                    throw new NotImplementedException();
            }
        }
    }

    enum TableHintKind
    {
        None,
        NoLock
    }

    class OffsetClause
    {
        public Literal OffsetExpression { get; set; }

        public Literal FetchExpression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent, int longestClauseLength)
        {
            buf.Append(' ', indent);
            buf.Append("OFFSET");
            buf.Append(' ', longestClauseLength - "OFFSET".Length + 1);
            OffsetExpression.ToString(buf, indent);
            buf.Append(" ROWS FETCH NEXT ");
            FetchExpression.ToString(buf, indent);
            buf.Append(" ROWS ONLY\r\n");
        }
    }

    class WhereClause
    {
        public BooleanExpression SearchCondition { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent, int longestClauseLength)
        {
            buf.Append(' ', indent);
            buf.Append("WHERE");
            buf.Append(' ', longestClauseLength - "WHERE".Length + 1);
            SearchCondition.ToString(buf, indent + longestClauseLength + 1);
            buf.Append("\r\n");
        }
    }

    abstract class SelectElement
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf, int indent);
    }

    class SelectStarExpression : SelectElement
    {
        public MultiPartIdentifier Qualifier { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            if (Qualifier != null)
            {
                Qualifier.ToString(buf, indent);
                buf.Append(".");
            }

            buf.Append("*");
        }
    }

    class SelectScalarExpression : SelectElement
    {
        public ScalarExpression Expression { get; set; }

        public IdentifierOrValueExpression ColumnName { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            Expression.ToString(buf, indent);

            if (ColumnName != null)
            {
                buf.Append(" AS ");
                ColumnName.ToString(buf);
            }
        }
    }

    abstract class ScalarExpression : Expression
    {
    }

    class ColumnReferenceExpression : ScalarExpression
    {
        public MultiPartIdentifier MultiPartIdentifier { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            MultiPartIdentifier.ToString(buf, indent);
        }
    }

    class FunctionCall : ScalarExpression
    {
        public Identifier FunctionName { get; set; }

        public List<ScalarExpression> Parameters { get; } = new List<ScalarExpression>();

        public UniqueRowFilter UniqueRowFilter { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FunctionName.ToString(buf);
            buf.Append("(");

            if (UniqueRowFilter == UniqueRowFilter.Distinct)
                buf.Append("DISTINCT ");

            for (var i = 0; i < Parameters.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                Parameters[i].ToString(buf, indent);
            }

            buf.Append(")");
        }
    }

    class ConvertCall : ScalarExpression
    {
        public SqlDataTypeReference DataType { get; set; }

        public ScalarExpression Parameter { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("CONVERT(");
            DataType.ToString(buf, indent);
            buf.Append(", ");
            Parameter.ToString(buf, indent);
            buf.Append(")");
        }
    }

    class SqlDataTypeReference
    {
        public SchemaObjectName Name { get; set; }

        public SqlDataTypeOption? SqlDataTypeOption { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            Name.ToString(buf, indent);
        }
    }

    enum SqlDataTypeOption
    {
        Float,
        DateTime
    }

    class IdentifierOrValueExpression
    {
        public Identifier Identifier { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf)
        {
            Identifier.ToString(buf);
        }
    }

    class GroupByClause
    {
        public List<ExpressionGroupingSpecification> GroupingSpecifications { get; } = new List<ExpressionGroupingSpecification>();

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent, int longestClauseLength)
        {
            buf.Append(' ', indent);
            buf.Append("GROUP BY");
            buf.Append(' ', longestClauseLength - "GROUP BY".Length + 1);

            for (var i = 0; i < GroupingSpecifications.Count; i++)
            {
                if (i > 0)
                {
                    buf.Append(",\r\n");
                    buf.Append(' ', indent + longestClauseLength + 1);
                }

                GroupingSpecifications[i].ToString(buf, indent);
            }

            buf.Append("\r\n");
        }
    }

    class ExpressionGroupingSpecification
    {
        public ScalarExpression Expression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            Expression.ToString(buf, indent);
        }
    }

    abstract class BooleanExpression : Expression
    {
    }

    class BooleanComparisonExpression : BooleanExpression
    {
        public Expression FirstExpression { get; set; }
        public Expression SecondExpression { get; set; }
        public BooleanComparisonType ComparisonType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstExpression.ToString(buf, indent);

            switch (ComparisonType)
            {
                case BooleanComparisonType.Equals:
                    buf.Append(" = ");
                    break;

                case BooleanComparisonType.GreaterThan:
                    buf.Append(" > ");
                    break;

                case BooleanComparisonType.GreaterThanOrEqualTo:
                    buf.Append(" >= ");
                    break;

                case BooleanComparisonType.LessThan:
                    buf.Append(" < ");
                    break;

                case BooleanComparisonType.LessThanOrEqualTo:
                    buf.Append(" <= ");
                    break;

                case BooleanComparisonType.NotEqualToBrackets:
                    buf.Append(" <> ");
                    break;

                default:
                    throw new NotImplementedException();
            }

            SecondExpression.ToString(buf, indent);
        }
    }

    enum BooleanComparisonType
    {
        Equals,
        GreaterThanOrEqualTo,
        GreaterThan,
        LessThanOrEqualTo,
        LessThan,
        NotEqualToBrackets
    }

    class BooleanParenthesisExpression : BooleanExpression
    {
        public BooleanExpression Expression { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("(");
            Expression.ToString(buf, Sql160ScriptGenerator.GetCurrentIndent(buf));
            buf.Append(")");
        }
    }

    class BooleanBinaryExpression : BooleanExpression
    {
        public BooleanExpression FirstExpression { get; set; }
        public BooleanExpression SecondExpression { get; set; }
        public BooleanBinaryExpressionType BinaryExpressionType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstExpression.ToString(buf, indent);

            buf.Append("\r\n");
            buf.Append(' ', indent);

            switch (BinaryExpressionType)
            {
                case BooleanBinaryExpressionType.And:
                    buf.Append("AND ");
                    break;

                case BooleanBinaryExpressionType.Or:
                    buf.Append("OR ");
                    break;

                default:
                    throw new NotImplementedException();
            }

            SecondExpression.ToString(buf, indent);
        }
    }

    enum BooleanBinaryExpressionType
    {
        And,
        Or
    }

    class LikePredicate : BooleanExpression
    {
        public Expression FirstExpression { get; set; }
        public Expression SecondExpression { get; set; }
        public bool NotDefined { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstExpression.ToString(buf, indent);

            if (NotDefined)
                buf.Append(" NOT LIKE ");
            else
                buf.Append(" LIKE ");

            SecondExpression.ToString(buf, indent);
        }
    }

    class BooleanTernaryExpression : BooleanExpression
    {
        public Expression FirstExpression { get; set; }
        public Expression SecondExpression { get; set; }
        public Expression ThirdExpression { get; set; }
        public BooleanTernaryExpressionType TernaryExpressionType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstExpression.ToString(buf, indent);

            switch (TernaryExpressionType)
            {
                case BooleanTernaryExpressionType.Between:
                    buf.Append(" BETWEEN ");
                    break;

                case BooleanTernaryExpressionType.NotBetween:
                    buf.Append(" NOT BETWEEN ");
                    break;

                default:
                    throw new NotImplementedException();
            }

            SecondExpression.ToString(buf, indent);
            buf.Append(" AND ");
            ThirdExpression.ToString(buf, indent);
        }
    }

    enum BooleanTernaryExpressionType
    {
        Between,
        NotBetween
    }

    class InPredicate : BooleanExpression
    {
        public Expression Expression { get; set; }
        public bool NotDefined { get; set; }
        public List<Expression> Values { get; } = new List<Expression>();
        public ScalarSubquery Subquery { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            Expression.ToString(buf, indent);

            if (NotDefined)
                buf.Append(" NOT IN (");
            else
                buf.Append(" IN (");

            if (Subquery != null)
            {
                Subquery.ToString(buf, Sql160ScriptGenerator.GetCurrentIndent(buf));
            }
            else
            {
                for (var i = 0; i < Values.Count; i++)
                {
                    if (i > 0)
                        buf.Append(", ");

                    Values[i].ToString(buf, indent);
                }
            }
            
            buf.Append(")");
        }
    }

    class FullTextPredicate : BooleanExpression
    {
        public List<ColumnReferenceExpression> Columns { get; } = new List<ColumnReferenceExpression>();

        public FullTextFunctionType FullTextFunctionType { get; set; }

        public StringLiteral Value { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            switch (FullTextFunctionType)
            {
                case FullTextFunctionType.Contains:
                    buf.Append("CONTAINS ");
                    break;

                default:
                    throw new NotSupportedException();
            }

            buf.Append("((");

            foreach (var col in Columns)
                col.ToString(buf, indent);

            buf.Append("), ");
            Value.ToString(buf, indent);
            buf.Append(")");
        }
    }

    enum FullTextFunctionType
    {
        Contains
    }

    class BooleanNotExpression : BooleanExpression
    {
        public BooleanExpression Expression { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("NOT ");
            Expression.ToString(buf, indent);
        }
    }

    class BinaryExpression : ScalarExpression
    {
        public ScalarExpression FirstExpression { get; set; }

        public BinaryExpressionType BinaryExpressionType { get; set; }

        public ScalarExpression SecondExpression { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstExpression.ToString(buf, indent);

            switch (BinaryExpressionType)
            {
                case BinaryExpressionType.Add:
                    buf.Append(" + ");
                    break;

                case BinaryExpressionType.Subtract:
                    buf.Append(" - ");
                    break;

                case BinaryExpressionType.Multiply:
                    buf.Append(" * ");
                    break;

                default:
                    throw new NotSupportedException();
            }

            SecondExpression.ToString(buf, indent);
        }
    }

    enum BinaryExpressionType
    {
        Add,
        Subtract,
        Multiply
    }

    class UnaryExpression : ScalarExpression
    {
        public ScalarExpression Expression { get; set; }

        public UnaryExpressionType UnaryExpressionType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            switch (UnaryExpressionType)
            {
                case UnaryExpressionType.Negative:
                    buf.Append("-");
                    break;

                default:
                    throw new NotSupportedException();
            }

            Expression.ToString(buf, indent);
        }
    }

    enum UnaryExpressionType
    {
        Negative
    }

    class ParameterlessCall : ScalarExpression
    {
        public ParameterlessCallType ParameterlessCallType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            switch (ParameterlessCallType)
            {
                case ParameterlessCallType.CurrentUser:
                    buf.Append("CURRENT_USER");
                    break;

                default:
                    throw new NotSupportedException();
            }
        }
    }

    enum ParameterlessCallType
    {
        CurrentUser
    }

    class ScalarSubquery
    {
        public QueryExpression QueryExpression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            QueryExpression.ToString(buf, indent);
        }
    }

    class BooleanIsNullExpression : BooleanExpression
    {
        public Expression Expression { get; set; }
        public bool IsNot { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            Expression.ToString(buf, indent);

            if (IsNot)
                buf.Append(" IS NOT NULL");
            else
                buf.Append(" IS NULL");
        }
    }

    class OrderByClause
    {
        public List<ExpressionWithSortOrder> OrderByElements { get; } = new List<ExpressionWithSortOrder>();

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent, int longestClauseLength)
        {
            buf.Append(' ', indent);
            buf.Append("ORDER BY");
            buf.Append(' ', longestClauseLength - "ORDER BY".Length + 1);

            for (var i = 0; i < OrderByElements.Count; i++)
            {
                if (i > 0)
                {
                    buf.Append(",\r\n");
                    buf.Append(' ', indent + longestClauseLength + 1);
                }

                OrderByElements[i].ToString(buf, indent + longestClauseLength + 1);
            }

            buf.Append("\r\n");
        }
    }

    class ExpressionWithSortOrder
    {
        public Expression Expression { get; set; }

        public SortOrder SortOrder { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            Expression.ToString(buf, indent);
            
            switch (SortOrder)
            {
                case SortOrder.Ascending:
                    buf.Append(" ASC");
                    break;

                case SortOrder.Descending:
                    buf.Append(" DESC");
                    break;

                default:
                    throw new NotImplementedException();
            }
        }
    }

    class WithCtesAndXmlNamespaces
    {
        public List<CommonTableExpression> CommonTableExpressions { get; } = new List<CommonTableExpression>();

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            buf.Append(' ', indent);
            buf.Append("WITH ");

            for (var i = 0; i < CommonTableExpressions.Count; i++)
            {
                if (i > 0)
                {
                    buf.Append(",\r\n");
                    buf.Append(' ', indent + 5);
                }

                CommonTableExpressions[i].ToString(buf, indent + 5);
            }

            buf.Append(" ");
        }
    }

    class CommonTableExpression
    {
        public Identifier ExpressionName { get; set; }

        public List<Identifier> Columns { get; } = new List<Identifier>();

        public QueryExpression QueryExpression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public void ToString(StringBuilder buf, int indent)
        {
            ExpressionName.ToString(buf);
            buf.Append("(");

            for (var i = 0; i < Columns.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                Columns[i].ToString(buf);
            }

            buf.Append(") AS (\r\n");
            QueryExpression.ToString(buf, indent);
            buf.Append(")");
        }
    }

    class ExistsPredicate : BooleanExpression
    {
        public ScalarSubquery Subquery { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("EXISTS(");
            Subquery?.ToString(buf, indent);
            buf.Append(")");
        }
    }

    class QueryDerivedTable : TableReference
    {
        public QueryExpression QueryExpression { get; set; }

        public Identifier Alias { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            buf.Append("(");
            QueryExpression?.ToString(buf, indent);
            buf.Append(")");

            if (Alias != null)
            {
                buf.Append(" AS ");
                Alias.ToString(buf);
            }
        }
    }

    class UnqualifiedJoin : TableReference
    {
        public TableReference FirstTableReference { get; set; }
        public UnqualifiedJoinType UnqualifiedJoinType { get; set; }
        public TableReference SecondTableReference { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf, int indent)
        {
            FirstTableReference?.ToString(buf, indent);
            buf.Append(" CROSS APPLY ");
            SecondTableReference?.ToString(buf, indent);
        }
    }

    enum SortOrder
    {
        Ascending,
        Descending
    }

    enum UnqualifiedJoinType
    {
        CrossApply
    }

    class TSqlFragmentVisitor
    {
        public virtual void ExplicitVisit(TSqlBatch node)
        {
            foreach (var statement in node.Statements)
                statement.Accept(this);
        }

        public virtual void ExplicitVisit(DeclareVariableStatement node)
        {
            foreach (var declaration in node.Declarations)
                declaration.Accept(this);
        }

        public virtual void ExplicitVisit(DeclareVariableElement node)
        {
            node.VariableName?.Accept(this);
            node.DataType?.Accept(this);
            node.Value?.Accept(this);
        }

        public virtual void ExplicitVisit(SelectStatement node)
        {
            node.QueryExpression?.Accept(this);
            node.WithCtesAndXmlNamespaces?.Accept(this);
        }

        public virtual void ExplicitVisit(MultiPartIdentifier node)
        {
            foreach (var identifier in node.Identifiers)
                identifier.Accept(this);
        }

        public virtual void ExplicitVisit(Identifier node)
        {
        }

        public virtual void ExplicitVisit(QuerySpecification node)
        {
            node.TopRowFilter?.Accept(this);

            foreach (var element in node.SelectElements)
                element.Accept(this);

            node.FromClause?.Accept(this);
            node.OffsetClause?.Accept(this);
            node.WhereClause?.Accept(this);
            node.GroupByClause?.Accept(this);
            node.OrderByClause?.Accept(this);
        }

        public virtual void ExplicitVisit(TopRowFilter node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(NullLiteral node)
        {
        }

        public virtual void ExplicitVisit(IntegerLiteral node)
        {
        }

        public virtual void ExplicitVisit(StringLiteral node)
        {
        }

        public virtual void ExplicitVisit(BinaryLiteral node)
        {
        }

        public virtual void ExplicitVisit(NumericLiteral node)
        {
        }

        public virtual void ExplicitVisit(MoneyLiteral node)
        {
        }

        public virtual void ExplicitVisit(FromClause node)
        {
            foreach (var table in node.TableReferences)
                table.Accept(this);
        }

        public virtual void ExplicitVisit(NamedTableReference node)
        {
            node.SchemaObject?.Accept(this);

            foreach (var hint in node.TableHints)
                hint.Accept(this);

            node.Alias?.Accept(this);
        }

        public virtual void ExplicitVisit(QualifiedJoin node)
        {
            node.FirstTableReference?.Accept(this);
            node.SecondTableReference?.Accept(this);
            node.SearchCondition?.Accept(this);
        }

        public virtual void ExplicitVisit(SchemaObjectName node)
        {
            foreach (var identifier in node.Identifiers)
                identifier.Accept(this);
        }

        public virtual void ExplicitVisit(OffsetClause node)
        {
            node.OffsetExpression?.Accept(this);
            node.FetchExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(WhereClause node)
        {
            node.SearchCondition?.Accept(this);
        }

        public virtual void ExplicitVisit(SelectStarExpression node)
        {
            node.Qualifier?.Accept(this);
        }

        public virtual void ExplicitVisit(SelectScalarExpression node)
        {
            node.Expression?.Accept(this);
            node.ColumnName?.Accept(this);
        }

        public virtual void ExplicitVisit(ColumnReferenceExpression node)
        {
            node.MultiPartIdentifier?.Accept(this);
        }

        public virtual void ExplicitVisit(FunctionCall node)
        {
            node.FunctionName?.Accept(this);

            foreach (var param in node.Parameters)
                param.Accept(this);
        }

        public virtual void ExplicitVisit(IdentifierOrValueExpression node)
        {
            node.Identifier?.Accept(this);
        }

        public virtual void ExplicitVisit(GroupByClause node)
        {
            foreach (var group in node.GroupingSpecifications)
                group.Accept(this);
        }

        public virtual void ExplicitVisit(ExpressionGroupingSpecification node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(BooleanComparisonExpression node)
        {
            node.FirstExpression?.Accept(this);
            node.SecondExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(BooleanBinaryExpression node)
        {
            node.FirstExpression?.Accept(this);
            node.SecondExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(LikePredicate node)
        {
            node.FirstExpression?.Accept(this);
            node.SecondExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(BooleanTernaryExpression node)
        {
            node.FirstExpression?.Accept(this);
            node.SecondExpression?.Accept(this);
            node.ThirdExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(InPredicate node)
        {
            node.Expression?.Accept(this);

            foreach (var value in node.Values)
                value.Accept(this);

            node.Subquery?.Accept(this);
        }

        public virtual void ExplicitVisit(BooleanIsNullExpression node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(OrderByClause node)
        {
            foreach (var order in node.OrderByElements)
                order.Accept(this);
        }

        public virtual void ExplicitVisit(ExpressionWithSortOrder node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(TableHint node)
        {
        }

        public virtual void ExplicitVisit(ConvertCall node)
        {
            node.DataType?.Accept(this);
            node.Parameter?.Accept(this);
        }

        public virtual void ExplicitVisit(SqlDataTypeReference node)
        {
            node.Name?.Accept(this);
        }

        public virtual void ExplicitVisit(VariableReference node)
        {
        }

        public virtual void ExplicitVisit(BooleanParenthesisExpression node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(WithCtesAndXmlNamespaces node)
        {
            foreach (var cte in node.CommonTableExpressions)
                cte.Accept(this);
        }

        public virtual void ExplicitVisit(CommonTableExpression node)
        {
            node.ExpressionName?.Accept(this);

            foreach (var col in node.Columns)
                col.Accept(this);

            node.QueryExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(ScalarSubquery node)
        {
            node.QueryExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(BinaryQueryExpression node)
        {
            node.FirstQueryExpression?.Accept(this);
            node.SecondQueryExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(BinaryExpression node)
        {
            node.FirstExpression?.Accept(this);
            node.SecondExpression?.Accept(this);
        }

        public virtual void ExplicitVisit(UnaryExpression node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(ParameterlessCall node)
        {
        }

        public virtual void ExplicitVisit(FullTextPredicate node)
        {
            foreach (var col in node.Columns)
                col.Accept(this);

            node.Value?.Accept(this);
        }

        public virtual void ExplicitVisit(BooleanNotExpression node)
        {
            node.Expression?.Accept(this);
        }

        public virtual void ExplicitVisit(ExistsPredicate node)
        {
            node.Subquery?.Accept(this);
        }

        public virtual void ExplicitVisit(QueryDerivedTable node)
        {
            node.QueryExpression?.Accept(this);
            node.Alias?.Accept(this);
        }

        public virtual void ExplicitVisit(UnqualifiedJoin node)
        {
            node.FirstTableReference?.Accept(this);
            node.SecondTableReference?.Accept(this);
        }
    }

    class Sql160ScriptGenerator
    {
        public void GenerateScript(TSqlBatch batch, out string sql)
        {
            var buf = new StringBuilder();
            batch.ToString(buf, 0);
            sql = buf.ToString().Trim();
        }

        internal static int GetCurrentIndent(StringBuilder buf)
        {
            var indent = 0;

            while (indent < buf.Length && buf[buf.Length - indent - 1] != '\n')
                indent++;

            return indent;
        }
    }
}
