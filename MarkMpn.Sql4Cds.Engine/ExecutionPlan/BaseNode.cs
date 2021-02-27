using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public abstract class BaseNode : IExecutionPlanNode
    {
        private int _executionCount;
        private Stopwatch _stopwatch = new Stopwatch();
        private int _rowsOut;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            IEnumerator<Entity> enumerator;

            try
            {
                _executionCount++;

                _stopwatch.Start();
                enumerator = ExecuteInternal(org, metadata, options, parameterTypes, parameterValues).GetEnumerator();
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
            finally
            {
                _stopwatch.Stop();
            }

            while (true)
            {
                Entity current;

                try
                {
                    _stopwatch.Start();
                    if (!enumerator.MoveNext())
                        break;

                    current = enumerator.Current;
                }
                catch (QueryExecutionException ex)
                {
                    if (ex.Node == null)
                        ex.Node = this;

                    throw;
                }
                catch (Exception ex)
                {
                    throw new QueryExecutionException(ex.Message, ex) { Node = this };
                }
                finally
                {
                    _stopwatch.Stop();
                }

                yield return current;
            }
        }

        public int ExecutionCount => _executionCount;

        public TimeSpan Duration => _stopwatch.Elapsed;

        public int RowsOut => _rowsOut;

        protected abstract IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues);

        public abstract IEnumerable<IExecutionPlanNode> GetSources();

        public abstract NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes);

        public abstract IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes);

        public abstract void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns);

        protected bool IsAliasReferenced(FetchXmlScan fetchXml, string alias)
        {
            var entity = fetchXml.FetchXml.Items.OfType<FetchEntityType>().Single();
            return IsAliasReferenced(entity.Items, alias);
        }

        protected bool IsAliasReferenced(object[] items, string alias)
        {
            if (items == null)
                return false;

            var hasSort = items.OfType<FetchOrderType>().Any(sort => sort.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

            if (hasSort)
                return true;

            var hasCondition = items.OfType<condition>().Any(condition => condition.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

            if (hasCondition)
                return true;

            foreach (var filter in items.OfType<filter>())
            {
                if (IsAliasReferenced(filter.Items, alias))
                    return true;
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (IsAliasReferenced(linkEntity.Items, alias))
                    return true;
            }

            return false;
        }

        protected FetchAttributeType AddAttribute(FetchXmlScan fetchXml, string colName, Func<FetchAttributeType, bool> predicate, IAttributeMetadataCache metadata, out bool added)
        {
            var entity = fetchXml.FetchXml.Items.OfType<FetchEntityType>().Single();
            var parts = colName.Split('.');

            if (parts.Length == 1)
            {
                added = false;
                return FindAliasedAttribute(entity.Items, colName, predicate);
            }

            var entityName = parts[0];
            var attr = new FetchAttributeType { name = parts[1] };

            if (fetchXml.Alias == entityName)
            {
                var meta = metadata[fetchXml.Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name);
                if (meta?.AttributeOf != null)
                    attr.name = meta.AttributeOf;

                if (entity.Items != null)
                {
                    var existing = entity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name);
                    if (existing != null && (predicate == null || predicate(existing)))
                    {
                        added = false;
                        return existing;
                    }
                }

                entity.AddItem(attr);
            }
            else
            {
                var linkEntity = entity.FindLinkEntity(entityName);

                var meta = metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name);
                if (meta?.AttributeOf != null)
                    attr.name = meta.AttributeOf;

                if (linkEntity.Items != null)
                {
                    var existing = linkEntity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name);
                    if (existing != null && (predicate == null || predicate(existing)))
                    {
                        added = false;
                        return existing;
                    }
                }

                linkEntity.AddItem(attr);
            }

            fetchXml.FetchXml = fetchXml.FetchXml;

            added = true;
            return attr;
        }

        protected FetchAttributeType FindAliasedAttribute(object[] items, string colName, Func<FetchAttributeType, bool> predicate)
        {
            if (items == null)
                return null;

            var match = items.OfType<FetchAttributeType>()
                .Where(a => a.alias == colName && (predicate == null || predicate(a)))
                .FirstOrDefault();

            if (match != null)
                return match;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                match = FindAliasedAttribute(linkEntity.Items, colName, predicate);

                if (match != null)
                    return match;
            }

            return null;
        }

        protected bool TranslateCriteria(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, out filter filter)
        {
            if (!TranslateCriteria(metadata, options, criteria, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var condition, out filter))
                return false;

            if (condition != null)
                filter = new filter { Items = new object[] { condition } };

            return true;
        }

        protected bool TranslateCriteria(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, out condition condition, out filter filter)
        {
            condition = null;
            filter = null;

            if (criteria is BooleanBinaryExpression binary)
            {
                if (!TranslateCriteria(metadata, options, binary.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var lhsCondition, out var lhsFilter))
                    return false;
                if (!TranslateCriteria(metadata, options, binary.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var rhsCondition, out var rhsFilter))
                    return false;

                filter = new filter
                {
                    type = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? filterType.and : filterType.or,
                    Items = new[]
                    {
                        (object) lhsCondition ?? lhsFilter,
                        (object) rhsCondition ?? rhsFilter
                    }
                };
                return true;
            }

            if (criteria is BooleanComparisonExpression comparison)
            {
                // Handle most comparison operators (=, <> etc.)
                // Comparison can be between a column and either a literal value, function call or another column (for joins only)
                // Function calls are used to represent more complex FetchXML query operators
                // Operands could be in either order, so `column = 'value'` or `'value' = column` should both be allowed
                var field = comparison.FirstExpression as ColumnReferenceExpression;
                var literal = comparison.SecondExpression as Literal;
                var func = comparison.SecondExpression as FunctionCall;
                var field2 = comparison.SecondExpression as ColumnReferenceExpression;
                var variable = comparison.SecondExpression as VariableReference;
                var expr = comparison.SecondExpression;
                var type = comparison.ComparisonType;

                if (field != null && field2 != null)
                {
                    // The operator is comparing two attributes. This is allowed in join criteria,
                    // but not in filter conditions before version 9.1.0.19251
                    if (!options.ColumnComparisonAvailable)
                        return false;
                }

                // If we couldn't find the pattern `column = value` or `column = func()`, try looking in the opposite order
                if (field == null && literal == null && func == null && variable == null)
                {
                    field = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression as Literal;
                    func = comparison.FirstExpression as FunctionCall;
                    variable = comparison.FirstExpression as VariableReference;
                    expr = comparison.FirstExpression;
                    field2 = null;

                    // Switch the operator depending on the order of the column and value, so `column > 3` uses gt but `3 > column` uses le
                    switch (type)
                    {
                        case BooleanComparisonType.GreaterThan:
                            type = BooleanComparisonType.LessThan;
                            break;

                        case BooleanComparisonType.GreaterThanOrEqualTo:
                            type = BooleanComparisonType.LessThanOrEqualTo;
                            break;

                        case BooleanComparisonType.LessThan:
                            type = BooleanComparisonType.GreaterThan;
                            break;

                        case BooleanComparisonType.LessThanOrEqualTo:
                            type = BooleanComparisonType.GreaterThanOrEqualTo;
                            break;
                    }
                }

                // If we still couldn't find the column name and value, this isn't a pattern we can support in FetchXML
                if (field == null || (literal == null && func == null && variable == null && (field2 == null || !options.ColumnComparisonAvailable) && !IsConstantValueExpression(expr, schema, out literal)))
                    return false;

                // Select the correct FetchXML operator
                @operator op;

                switch (type)
                {
                    case BooleanComparisonType.Equals:
                        op = @operator.eq;
                        break;

                    case BooleanComparisonType.GreaterThan:
                        op = @operator.gt;
                        break;

                    case BooleanComparisonType.GreaterThanOrEqualTo:
                        op = @operator.ge;
                        break;

                    case BooleanComparisonType.LessThan:
                        op = @operator.lt;
                        break;

                    case BooleanComparisonType.LessThanOrEqualTo:
                        op = @operator.le;
                        break;

                    case BooleanComparisonType.NotEqualToBrackets:
                    case BooleanComparisonType.NotEqualToExclamation:
                        op = @operator.ne;
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unsupported comparison type", comparison);
                }

                string value = null;
                List<string> values = null;

                if (literal != null)
                {
                    value = literal.Value;
                }
                else if (variable != null)
                {
                    value = variable.Name;
                }
                else if (func != null && Enum.TryParse<@operator>(func.FunctionName.Value.ToLower(), out var customOperator))
                {
                    if (op == @operator.eq)
                    {
                        // If we've got the pattern `column = func()`, select the FetchXML operator from the function name
                        op = customOperator;

                        // Check for unsupported SQL DOM elements within the function call
                        if (func.CallTarget != null)
                            throw new NotSupportedQueryFragmentException("Unsupported function call target", func);

                        if (func.Collation != null)
                            throw new NotSupportedQueryFragmentException("Unsupported function collation", func);

                        if (func.OverClause != null)
                            throw new NotSupportedQueryFragmentException("Unsupported function OVER clause", func);

                        if (func.UniqueRowFilter != UniqueRowFilter.NotSpecified)
                            throw new NotSupportedQueryFragmentException("Unsupported function unique filter", func);

                        if (func.WithinGroupClause != null)
                            throw new NotSupportedQueryFragmentException("Unsupported function group clause", func);

                        if (func.Parameters.Count > 1 && op != @operator.containvalues && op != @operator.notcontainvalues)
                            throw new NotSupportedQueryFragmentException("Unsupported number of function parameters", func);

                        // Some advanced FetchXML operators use a value as well - take this as the function parameter
                        // This provides support for queries such as `createdon = lastxdays(3)` becoming <condition attribute="createdon" operator="last-x-days" value="3" />
                        if (op == @operator.containvalues || op == @operator.notcontainvalues)
                        {
                            values = new List<string>();

                            foreach (var funcParam in func.Parameters)
                            {
                                if (!(funcParam is Literal paramLiteral))
                                    throw new NotSupportedQueryFragmentException("Unsupported function parameter", funcParam);

                                values.Add(paramLiteral.Value);
                            }
                        }
                        else if (func.Parameters.Count == 1)
                        {
                            if (!(func.Parameters[0] is Literal paramLiteral))
                                throw new NotSupportedQueryFragmentException("Unsupported function parameter", func.Parameters[0]);

                            value = paramLiteral.Value;
                        }
                    }
                    else
                    {
                        // Can't use functions with other operators
                        throw new NotSupportedQueryFragmentException("Unsupported function use. Only <field> = <func>(<param>) usage is supported", comparison);
                    }
                }
                else if (func != null)
                {
                    if (IsConstantValueExpression(func, schema, out literal))
                        value = literal.Value;
                    else
                        throw new PostProcessingRequiredException("Unsupported FetchXML function", func);
                }

                // Find the entity that the condition applies to, which may be different to the entity that the condition FetchXML element will be 
                // added within
                var columnName = field.GetColumnName();
                if (!schema.ContainsColumn(columnName, out columnName))
                    return false;

                var parts = columnName.Split('.');
                var entityName = parts[0];
                var attrName = parts[1];

                if (allowedPrefix != null && !allowedPrefix.Equals(entityName))
                    return false;

                var meta = metadata[entityName];

                if (field2 == null)
                {
                    var attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                    if (!String.IsNullOrEmpty(attribute?.AttributeOf) && meta.Attributes.Any(a => a.LogicalName == attribute.AttributeOf))
                    {
                        var baseAttribute = meta.Attributes.Single(a => a.LogicalName == attribute.AttributeOf);
                        var virtualAttributeHandled = false;

                        // If filtering on the display name of an optionset attribute, convert it to filtering on the underlying value field
                        // instead where possible.
                        if (attribute.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is EnumAttributeMetadata enumAttr)
                        {
                            var matchingOptions = enumAttr.OptionSet.Options.Where(o => o.Label.UserLocalizedLabel.Label.Equals(value, StringComparison.OrdinalIgnoreCase)).ToList();

                            if (matchingOptions.Count == 1)
                            {
                                attrName = baseAttribute.LogicalName;
                                value = matchingOptions[0].Value.ToString();
                                virtualAttributeHandled = true;
                            }
                            else if (matchingOptions.Count == 0 && (op == @operator.eq || op == @operator.ne || op == @operator.neq))
                            {
                                throw new NotSupportedQueryFragmentException("Unknown optionset value. Supported values are " + String.Join(", ", enumAttr.OptionSet.Options.Select(o => o.Label.UserLocalizedLabel.Label)), literal);
                            }
                        }

                        // If filtering on the display name of a lookup value, add a join to the target type and filter
                        // on the primary name attribute instead.
                        if (attribute.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is LookupAttributeMetadata lookupAttr && lookupAttr.Targets.Length == 1)
                        {
                            // TODO:
                            return false;
                            /*
                            var targetMetadata = Metadata[lookupAttr.Targets[0]];
                            var join = entityTable.GetItems().OfType<FetchLinkEntityType>().FirstOrDefault(link => link.name == targetMetadata.LogicalName && link.from == targetMetadata.PrimaryIdAttribute && link.to == baseAttribute.LogicalName && link.linktype == "outer");

                            if (join == null)
                            {
                                join = new FetchLinkEntityType
                                {
                                    name = targetMetadata.LogicalName,
                                    from = targetMetadata.PrimaryIdAttribute,
                                    to = baseAttribute.LogicalName,
                                    alias = $"{entityTable.EntityName}_{baseAttribute.LogicalName}",
                                    linktype = "outer"
                                };
                                var joinTable = new EntityTable(Metadata, join) { Hidden = true };
                                tables.Add(joinTable);

                                entityTable.AddItem(join);
                                entityTable = joinTable;
                            }
                            else
                            {
                                entityTable = tables.Single(t => t.LinkEntity == join);
                            }

                            entityName = entityTable.Alias;
                            attrName = targetMetadata.PrimaryNameAttribute;
                            virtualAttributeHandled = true;
                            */
                        }

                        if (!virtualAttributeHandled)
                            throw new PostProcessingRequiredException("Cannot filter on virtual name attributes", field);
                    }

                    if (!Int32.TryParse(value, out _) && attribute?.AttributeType == AttributeTypeCode.EntityName)
                    {
                        // Convert the entity name to the object type code
                        var targetMetadata = metadata[value];

                        value = targetMetadata.ObjectTypeCode?.ToString();
                    }

                    condition = new condition
                    {
                        entityname = entityName == targetEntityName ? null : entityName,
                        attribute = attrName,
                        @operator = op,
                        value = value
                    };
                    return true;
                }
                else
                {
                    // Column comparisons can only happen within a single entity
                    var columnName2 = field2.GetColumnName();
                    if (!schema.ContainsColumn(columnName2, out columnName2))
                        return false;

                    var parts2 = columnName2.Split('.');
                    var entityName2 = parts2[0];
                    var attrName2 = parts2[1];

                    if (!entityName.Equals(entityName2))
                        return false;

                    var attr1 = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));
                    var attr2 = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName2, StringComparison.OrdinalIgnoreCase));

                    if (!String.IsNullOrEmpty(attr1?.AttributeOf))
                        return false;

                    if (!String.IsNullOrEmpty(attr2?.AttributeOf))
                        return false;

                    condition = new condition
                    {
                        entityname = entityName == targetEntityName ? null : entityName,
                        attribute = attrName,
                        @operator = op,
                        valueof = attrName2
                    };
                    return true;
                }
            }

            if (criteria is InPredicate inPred)
            {
                // Checking if a column is in a list of literals is foldable, everything else isn't
                if (!(inPred.Expression is ColumnReferenceExpression inCol))
                    return false;

                if (inPred.Subquery != null)
                    return false;

                if (!inPred.Values.All(v => v is Literal))
                    return false;

                var columnName = inCol.GetColumnName();

                if (!schema.ContainsColumn(columnName, out columnName))
                    return false;

                var parts = columnName.Split('.');
                var entityName = parts[0];
                var attrName = parts[1];

                if (allowedPrefix != null && !allowedPrefix.Equals(entityName))
                    return false;

                condition = new condition
                {
                    entityname = entityName == targetEntityName ? null : entityName,
                    attribute = attrName,
                    @operator = inPred.NotDefined ? @operator.notin : @operator.@in,
                    Items = inPred.Values.Cast<Literal>().Select(lit => new conditionValue { Value = lit.Value }).ToArray()
                };
                return true;
            }

            if (criteria is BooleanIsNullExpression isNull)
            {
                if (!(isNull.Expression is ColumnReferenceExpression nullCol))
                    return false;

                var columnName = nullCol.GetColumnName();

                if (!schema.ContainsColumn(columnName, out columnName))
                    return false;

                var parts = columnName.Split('.');
                var entityName = parts[0];
                var attrName = parts[1];

                if (allowedPrefix != null && !allowedPrefix.Equals(entityName))
                    return false;

                condition = new condition
                {
                    entityname = entityName == targetEntityName ? null : entityName,
                    attribute = attrName,
                    @operator = isNull.IsNot ? @operator.notnull : @operator.@null
                };
                return true;
            }

            return false;
        }

        protected bool IsConstantValueExpression(ScalarExpression expr, NodeSchema schema, out Literal literal)
        {
            literal = null;

            var columnVisitor = new ColumnCollectingVisitor();
            expr.Accept(columnVisitor);

            if (columnVisitor.Columns.Count > 0)
                return false;

            var variableVisitor = new VariableCollectingVisitor();
            expr.Accept(variableVisitor);

            if (variableVisitor.Variables.Count > 0)
                return false;

            var value = expr.GetValue(null, schema, null, null);

            if (value is int i)
                literal = new IntegerLiteral { Value = i.ToString() };
            else if (value == null)
                literal = new NullLiteral();
            else if (value is decimal dec)
                literal = new NumericLiteral { Value = dec.ToString() };
            else if (value is double dbl)
                literal = new NumericLiteral { Value = dbl.ToString() };
            else if (value is float flt)
                literal = new RealLiteral { Value = flt.ToString() };
            else if (value is string str)
                literal = new StringLiteral { Value = str };
            else if (value is DateTime dt)
                literal = new StringLiteral { Value = dt.ToString("o") };
            else if (value is EntityReference er)
                literal = new StringLiteral { Value = er.Id.ToString() };
            else if (value is Guid g)
                literal = new StringLiteral { Value = g.ToString() };
            else
                return false;

            return true;
        }

        public override string ToString()
        {
            return System.Text.RegularExpressions.Regex.Replace(GetType().Name.Replace("Node", ""), "([a-z])([A-Z])", "$1 $2");
        }
    }
}
