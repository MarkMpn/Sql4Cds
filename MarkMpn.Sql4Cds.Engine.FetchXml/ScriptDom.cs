using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/// <summary>
/// Basic shims for ScriptDom classes required to build a SQL statement from FetchXml
/// if the full ScriptDom assembly is not available
/// </summary>
namespace Microsoft.SqlServer.TransactSql.ScriptDom
{
    class SelectStatement
    {
        public QuerySpecification QueryExpression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
            QueryExpression?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("SELECT ");
            QueryExpression?.ToString(buf);
        }
    }

    class QuerySpecification
    {
        public TopRowFilter TopRowFilter { get; set; }

        public UniqueRowFilter UniqueRowFilter { get; set; }

        public List<SelectElement> SelectElements { get; } = new List<SelectElement>();

        public FromClause FromClause { get; set; }

        public WhereClause WhereClause { get; set; }

        public GroupByClause GroupByClause { get; set; }

        public OrderByClause OrderByClause { get; set; }

        public OffsetClause OffsetClause { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
            TopRowFilter?.Accept(visitor);

            foreach (var element in SelectElements)
                element.Accept(visitor);

            FromClause?.Accept(visitor);
            OffsetClause?.Accept(visitor);
            WhereClause?.Accept(visitor);
            GroupByClause?.Accept(visitor);
            OrderByClause?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            TopRowFilter?.ToString(buf);

            if (UniqueRowFilter == UniqueRowFilter.Distinct)
                buf.Append("DISTINCT ");

            for (var i = 0; i < SelectElements.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                SelectElements[i].ToString(buf);
            }
            buf.Append(" ");

            FromClause?.ToString(buf);
            WhereClause?.ToString(buf);
            GroupByClause?.ToString(buf);
            OrderByClause?.ToString(buf);
            OffsetClause?.ToString(buf);
        }
    }

    class TopRowFilter
    {
        public Expression Expression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
            Expression?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("TOP ");
            Expression?.ToString(buf);
            buf.Append(" ");
        }
    }

    abstract class Expression
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf);
    }

    abstract class Literal : ScalarExpression
    {
        public string Value { get; set; }

        public override void ToString(StringBuilder buf)
        {
            buf.Append(Value);
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

        public override void ToString(StringBuilder buf)
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

    class IdentifierLiteral : Literal
    {
        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf)
        {
            buf.Append("'");
            buf.Append(Value.Replace("'", "''"));
            buf.Append("'");
        }
    }

    class VariableReference : ScalarExpression
    {
        public string Name { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);
        }

        public override void ToString(StringBuilder buf)
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

            foreach (var table in TableReferences)
                table.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("FROM ");

            for (var i = 0; i < TableReferences.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                TableReferences[i].ToString(buf);
            }

            buf.Append(" ");
        }
    }

    abstract class TableReference
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf);
    }

    class NamedTableReference : TableReference
    {
        public SchemaObjectName SchemaObject { get; set; }

        public List<TableHint> TableHints { get; } = new List<TableHint>();

        public Identifier Alias { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            SchemaObject?.Accept(visitor);

            foreach (var hint in TableHints)
                hint.Accept(visitor);

            Alias?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            SchemaObject.ToString(buf);

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

            FirstTableReference?.Accept(visitor);
            SecondTableReference?.Accept(visitor);
            SearchCondition?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FirstTableReference.ToString(buf);

            if (QualifiedJoinType == QualifiedJoinType.Inner)
                buf.Append(" INNER JOIN ");
            else
                buf.Append(" LEFT OUTER JOIN ");

            SecondTableReference.ToString(buf);
            buf.Append(" ON ");
            SearchCondition.ToString(buf);
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

            foreach (var identifier in Identifiers)
                identifier.Accept(visitor);
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

            foreach (var identifier in Identifiers)
                identifier.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
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

            OffsetExpression?.Accept(visitor);
            FetchExpression?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("OFFSET ");
            OffsetExpression.ToString(buf);
            buf.Append(" ROWS FETCH NEXT ");
            FetchExpression.ToString(buf);
            buf.Append(" ROWS ONLY ");
        }
    }

    class WhereClause
    {
        public BooleanExpression SearchCondition { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            SearchCondition?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("WHERE ");
            SearchCondition.ToString(buf);
            buf.Append(" ");
        }
    }

    abstract class SelectElement
    {
        public abstract void Accept(TSqlFragmentVisitor visitor);

        public abstract void ToString(StringBuilder buf);
    }

    class SelectStarExpression : SelectElement
    {
        public MultiPartIdentifier Qualifier { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Qualifier?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            if (Qualifier != null)
            {
                Qualifier.ToString(buf);
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

            Expression?.Accept(visitor);
            ColumnName?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            Expression.ToString(buf);

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

            MultiPartIdentifier?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            MultiPartIdentifier.ToString(buf);
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

            FunctionName?.Accept(visitor);

            foreach (var param in Parameters)
                param.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FunctionName.ToString(buf);
            buf.Append("(");

            if (UniqueRowFilter == UniqueRowFilter.Distinct)
                buf.Append("DISTINCT ");

            for (var i = 0; i < Parameters.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                Parameters[i].ToString(buf);
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

            DataType?.Accept(visitor);
            Parameter?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            buf.Append("CONVERT(");
            DataType.ToString(buf);
            buf.Append(", ");
            Parameter.ToString(buf);
            buf.Append(")");
        }
    }

    class SqlDataTypeReference
    {
        public SchemaObjectName Name { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Name?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            Name.ToString(buf);
        }
    }

    class IdentifierOrValueExpression
    {
        public Identifier Identifier { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Identifier?.Accept(visitor);
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

            foreach (var group in GroupingSpecifications)
                group.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("GROUP BY ");

            for (var i = 0; i < GroupingSpecifications.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                GroupingSpecifications[i].ToString(buf);
            }

            buf.Append(" ");
        }
    }

    class ExpressionGroupingSpecification
    {
        public ScalarExpression Expression { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Expression?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            Expression.ToString(buf);
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

            FirstExpression?.Accept(visitor);
            SecondExpression?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FirstExpression.ToString(buf);

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

            SecondExpression.ToString(buf);
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

    class BooleanBinaryExpression : BooleanExpression
    {
        public Expression FirstExpression { get; set; }
        public Expression SecondExpression { get; set; }
        public BooleanBinaryExpressionType BinaryExpressionType { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            FirstExpression?.Accept(visitor);
            SecondExpression?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FirstExpression.ToString(buf);

            switch (BinaryExpressionType)
            {
                case BooleanBinaryExpressionType.And:
                    buf.Append(" AND ");
                    break;

                case BooleanBinaryExpressionType.Or:
                    buf.Append(" OR ");
                    break;

                default:
                    throw new NotImplementedException();
            }

            SecondExpression.ToString(buf);
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

            FirstExpression?.Accept(visitor);
            SecondExpression?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FirstExpression.ToString(buf);

            if (NotDefined)
                buf.Append(" NOT LIKE ");
            else
                buf.Append(" LIKE ");

            SecondExpression.ToString(buf);
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

            FirstExpression?.Accept(visitor);
            SecondExpression?.Accept(visitor);
            ThirdExpression?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            FirstExpression.ToString(buf);

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

            SecondExpression.ToString(buf);
            buf.Append(" AND ");
            ThirdExpression.ToString(buf);
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

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Expression?.Accept(visitor);

            foreach (var value in Values)
                value.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            Expression.ToString(buf);

            if (NotDefined)
                buf.Append(" NOT IN (");
            else
                buf.Append(" IN (");

            for (var i = 0; i < Values.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                Values[i].ToString(buf);
            }
            
            buf.Append(")");
        }
    }

    class BooleanIsNullExpression : BooleanExpression
    {
        public Expression Expression { get; set; }
        public bool IsNot { get; set; }

        public override void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Expression?.Accept(visitor);
        }

        public override void ToString(StringBuilder buf)
        {
            Expression.ToString(buf);

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

            foreach (var order in OrderByElements)
                order.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            buf.Append("ORDER BY ");

            for (var i = 0; i < OrderByElements.Count; i++)
            {
                if (i > 0)
                    buf.Append(", ");

                OrderByElements[i].ToString(buf);
            }

            buf.Append(" ");
        }
    }

    class ExpressionWithSortOrder
    {
        public Expression Expression { get; set; }

        public SortOrder SortOrder { get; set; }

        public void Accept(TSqlFragmentVisitor visitor)
        {
            visitor.ExplicitVisit(this);

            Expression?.Accept(visitor);
        }

        public void ToString(StringBuilder buf)
        {
            Expression.ToString(buf);
            
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

    enum SortOrder
    {
        Ascending,
        Descending
    }

    class TSqlFragmentVisitor
    {
        public virtual void ExplicitVisit(SelectStatement node)
        {
        }

        public virtual void ExplicitVisit(MultiPartIdentifier node)
        {
        }

        public virtual void ExplicitVisit(Identifier node)
        {
        }

        public virtual void ExplicitVisit(QuerySpecification querySpecification)
        {
        }

        public virtual void ExplicitVisit(TopRowFilter topRowFilter)
        {
        }

        public virtual void ExplicitVisit(IntegerLiteral integerLiteral)
        {
        }

        public virtual void ExplicitVisit(StringLiteral stringLiteral)
        {
        }

        public virtual void ExplicitVisit(BinaryLiteral binaryLiteral)
        {
        }

        public virtual void ExplicitVisit(NumericLiteral numericLiteral)
        {
        }

        public virtual void ExplicitVisit(MoneyLiteral moneyLiteral)
        {
        }

        public virtual void ExplicitVisit(IdentifierLiteral identifierLiteral)
        {
        }

        public virtual void ExplicitVisit(FromClause fromClause)
        {
        }

        public virtual void ExplicitVisit(NamedTableReference namedTableReference)
        {
        }

        public virtual void ExplicitVisit(QualifiedJoin qualifiedJoin)
        {
        }

        public virtual void ExplicitVisit(SchemaObjectName schemaObjectName)
        {
        }

        public virtual void ExplicitVisit(OffsetClause offsetClause)
        {
        }

        public virtual void ExplicitVisit(WhereClause whereClause)
        {
        }

        public virtual void ExplicitVisit(SelectStarExpression selectStarExpression)
        {
        }

        public virtual void ExplicitVisit(SelectScalarExpression selectScalarExpression)
        {
        }

        public virtual void ExplicitVisit(ColumnReferenceExpression columnReferenceExpression)
        {
        }

        public virtual void ExplicitVisit(FunctionCall functionCall)
        {
        }

        public virtual void ExplicitVisit(IdentifierOrValueExpression identifierOrValueExpression)
        {
        }

        public virtual void ExplicitVisit(GroupByClause groupByClause)
        {
        }

        public virtual void ExplicitVisit(ExpressionGroupingSpecification expressionGroupingSpecification)
        {
        }

        public virtual void ExplicitVisit(BooleanComparisonExpression booleanComparisonExpression)
        {
        }

        public virtual void ExplicitVisit(BooleanBinaryExpression booleanBinaryExpression)
        {
        }

        public virtual void ExplicitVisit(LikePredicate likePredicate)
        {
        }

        public virtual void ExplicitVisit(BooleanTernaryExpression booleanTernaryExpression)
        {
        }

        public virtual void ExplicitVisit(InPredicate inPredicate)
        {
        }

        public virtual void ExplicitVisit(BooleanIsNullExpression booleanIsNullExpression)
        {
        }

        public virtual void ExplicitVisit(OrderByClause orderByClause)
        {
        }

        public virtual void ExplicitVisit(ExpressionWithSortOrder expressionWithSortOrder)
        {
        }

        public virtual void ExplicitVisit(TableHint tableHint)
        {
        }

        internal void ExplicitVisit(ConvertCall convertCall)
        {
        }

        internal void ExplicitVisit(SqlDataTypeReference sqlDataTypeReference)
        {
        }

        internal void ExplicitVisit(VariableReference variableReference)
        {
        }
    }

    class Sql150ScriptGenerator
    {
        public void GenerateScript(SelectStatement statement, out string sql)
        {
            var buf = new StringBuilder();
            statement.ToString(buf);
            sql = buf.ToString().Trim();
        }
    }
}
