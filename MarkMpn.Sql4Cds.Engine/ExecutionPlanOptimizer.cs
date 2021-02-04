using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Optimizes an execution plan once it has been built
    /// </summary>
    public class ExecutionPlanOptimizer
    {
        public ExecutionPlanOptimizer(IAttributeMetadataCache metadata, bool columnComparisonAvailable)
        {
            Metadata = metadata;
            ColumnComparisonAvailable = columnComparisonAvailable;
        }

        public IAttributeMetadataCache Metadata { get; }

        public bool ColumnComparisonAvailable { get; }

        /// <summary>
        /// Optimizes an execution plan
        /// </summary>
        /// <param name="node">The root node of the execution plan</param>
        /// <returns>A new execution plan node</returns>
        public IExecutionPlanNode Optimize(IExecutionPlanNode node)
        {
            // Move any additional operators down to the FetchXml
            node = MergeNodeDown(node);

            // Push required column names down to leaf node data sources so only the required data is exported
            PushColumnsDown(node, new List<string>());

            //Sort the items in the FetchXml nodes to match examples in documentation
            SortFetchXmlElements(node);

            return node;
        }

        private void SortFetchXmlElements(IExecutionPlanNode node)
        {
            if (node is FetchXmlScan fetchXml)
                SortFetchXmlElements(fetchXml.FetchXml.Items);

            foreach (var source in node.GetSources())
                SortFetchXmlElements(source);
        }

        private void SortFetchXmlElements(object[] items)
        {
            if (items == null)
                return;

            items.StableSort(new FetchXmlElementComparer());

            foreach (var entity in items.OfType<FetchEntityType>())
                SortFetchXmlElements(entity.Items);

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                SortFetchXmlElements(linkEntity.Items);
        }

        private void PushColumnsDown(IExecutionPlanNode node, List<string> columns)
        {
            if (node is FetchXmlScan fetchXmlNode)
            {
                // Add columns to FetchXml
                var entity = fetchXmlNode.FetchXml.Items.OfType<FetchEntityType>().Single();

                var items = new List<object>();
                if (entity.Items != null)
                    items.AddRange(entity.Items);

                foreach (var col in columns.Where(c => c.StartsWith(fetchXmlNode.Alias + ".")).Select(c => c.Split('.')[1]).Distinct())
                {
                    if (col == "*")
                        items.Add(new allattributes());
                    else if (!items.OfType<FetchAttributeType>().Any(a => a.alias == col || (a.alias == null && a.name == col)))
                        items.Add(new FetchAttributeType { name = col });
                }

                if (items.OfType<allattributes>().Any())
                {
                    items.Clear();
                    items.Add(new allattributes());
                }

                entity.Items = items.ToArray();
                fetchXmlNode.FetchXml = fetchXmlNode.FetchXml;
            }

            var schema = node.GetSchema(Metadata);
            var sourceRequiredColumns = new List<string>(columns);

            foreach (var col in node.GetRequiredColumns())
            {
                if (schema.Aliases.TryGetValue(col, out var aliasedCols))
                    sourceRequiredColumns.AddRange(aliasedCols);
                else
                    sourceRequiredColumns.Add(col);
            }

            foreach (var source in node.GetSources())
                PushColumnsDown(source, sourceRequiredColumns);
        }

        private IExecutionPlanNode MergeNodeDown(IExecutionPlanNode node)
        {
            if (node is SelectNode select)
            {
                select.Source = MergeNodeDown(select.Source);

                if (select.Source is FetchXmlScan fetchXml)
                {
                    // Check if there are any aliases we can apply to the source FetchXml
                    var schema = fetchXml.GetSchema(Metadata);

                    foreach (var col in select.ColumnSet)
                    {
                        var sourceCol = col.SourceColumn;
                        schema.ContainsColumn(sourceCol, out sourceCol);
                        var attr = AddAttribute(fetchXml, sourceCol, null, out var added);

                        if (col.SourceColumn != col.OutputColumn)
                        {
                            if (added)
                            {
                                attr.alias = col.OutputColumn;
                                col.SourceColumn = col.OutputColumn;
                            }
                            else
                            {
                                col.SourceColumn = attr.alias ?? (sourceCol.Split('.')[0] + "." + attr.name);
                            }
                        }
                    }
                }
            }
            else if (node is MergeJoinNode mergeJoin && (mergeJoin.JoinType == QualifiedJoinType.Inner || mergeJoin.JoinType == QualifiedJoinType.LeftOuter))
            {
                // Merge join has preceding Sort nodes to ensure data is in correct order. Fold them into the source
                // FetchXML only if the join itself can't be folded in.
                var left = mergeJoin.LeftSource;
                if (left is SortNode leftSort && leftSort.IgnoreForFetchXmlFolding)
                {
                    left = MergeNodeDown(leftSort.Source);
                    leftSort.Source = left;
                }

                var right = mergeJoin.RightSource;
                if (right is SortNode rightSort && rightSort.IgnoreForFetchXmlFolding)
                {
                    right = MergeNodeDown(rightSort.Source);
                    rightSort.Source = right;
                }

                var convertedJoin = false;

                try
                {
                    if (left is FetchXmlScan leftFetch && right is FetchXmlScan rightFetch)
                    {
                        var leftEntity = leftFetch.Entity;
                        var rightEntity = rightFetch.Entity;

                        // Check that the join is on columns that are available in the FetchXML
                        var leftSchema = left.GetSchema(Metadata);
                        var rightSchema = right.GetSchema(Metadata);
                        var leftAttribute = mergeJoin.LeftAttribute.GetColumnName();
                        if (!leftSchema.ContainsColumn(leftAttribute, out leftAttribute))
                            return node;
                        var rightAttribute = mergeJoin.RightAttribute.GetColumnName();
                        if (!rightSchema.ContainsColumn(rightAttribute, out rightAttribute))
                            return node;
                        var leftAttributeParts = leftAttribute.Split('.');
                        var rightAttributeParts = rightAttribute.Split('.');
                        if (leftAttributeParts.Length != 2)
                            return node;
                        if (rightAttributeParts.Length != 2)
                            return node;

                        // Must be joining to the root entity of the right source, i.e. not a child link-entity
                        if (!rightAttributeParts[0].Equals(rightFetch.Alias))
                            return node;

                        // If there are any additional join criteria, either they must be able to be translated to FetchXml criteria
                        // in the new link entity or we must be using an inner join so we can use a post-filter node
                        var additionalCriteria = mergeJoin.AdditionalJoinCriteria;

                        if (TranslateCriteria(additionalCriteria, rightSchema, rightFetch.Alias, rightEntity.name, rightFetch.Alias, out var filter))
                        {
                            if (rightEntity.Items == null)
                                rightEntity.Items = new object[] { filter };
                            else
                                rightEntity.Items = rightEntity.Items.Concat(new object[] { filter }).ToArray();

                            additionalCriteria = null;
                        }

                        if (additionalCriteria != null && mergeJoin.JoinType != QualifiedJoinType.Inner)
                            return node;

                        var rightLinkEntity = new FetchLinkEntityType
                        {
                            alias = rightFetch.Alias,
                            name = rightEntity.name,
                            linktype = mergeJoin.JoinType == QualifiedJoinType.Inner ? "inner" : "outer",
                            from = rightAttributeParts[1],
                            to = leftAttributeParts[1],
                            Items = rightEntity.Items
                        };

                        // Find where the two FetchXml documents should be merged together and return the merged version
                        if (leftAttributeParts[0].Equals(leftFetch.Alias))
                        {
                            if (leftEntity.Items == null)
                                leftEntity.Items = new object[] { rightLinkEntity };
                            else
                                leftEntity.Items = leftEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                        }
                        else
                        {
                            var leftLinkEntity = FindLinkEntity(leftFetch.FetchXml.Items.OfType<FetchEntityType>().Single().Items, leftAttributeParts[0]);

                            if (leftLinkEntity == null)
                                return node;

                            if (leftLinkEntity.Items == null)
                                leftLinkEntity.Items = new object[] { rightLinkEntity };
                            else
                                leftLinkEntity.Items = leftLinkEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                        }

                        convertedJoin = true;

                        if (additionalCriteria != null)
                            return new FilterNode { Filter = additionalCriteria, Source = leftFetch };

                        return leftFetch;
                    }
                }
                finally
                {
                    if (!convertedJoin)
                    {
                        mergeJoin.LeftSource = MergeNodeDown(mergeJoin.LeftSource);
                        mergeJoin.RightSource = MergeNodeDown(mergeJoin.RightSource);
                    }
                }
            }
            else if (node is FilterNode filter)
            {
                filter.Source = MergeNodeDown(filter.Source);

                if (filter.Source is FetchXmlScan fetchXml)
                {
                    if (TranslateCriteria(filter.Filter, fetchXml.GetSchema(Metadata), null, fetchXml.Entity.name, fetchXml.Alias, out var fetchFilter))
                    {
                        fetchXml.Entity.AddItem(fetchFilter);
                        return fetchXml;
                    }
                }
            }
            else if (node is SortNode sort)
            {
                sort.Source = MergeNodeDown(sort.Source);

                if (sort.Source is FetchXmlScan fetchXml)
                {
                    var schema = fetchXml.GetSchema(Metadata);
                    var entity = fetchXml.Entity;
                    var items = entity.Items;

                    foreach (var sortOrder in sort.Sorts)
                    {
                        if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                            return sort;

                        if (!schema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                            return sort;

                        var parts = sortCol.Split('.');
                        var entityName = parts[0];
                        var attrName = parts[1];

                        var fetchSort = new FetchOrderType { attribute = attrName, descending = sortOrder.SortOrder == SortOrder.Descending };
                        if (entityName == fetchXml.Alias)
                        {
                            if (items != entity.Items)
                                return sort;

                            entity.AddItem(fetchSort);
                            items = entity.Items;
                        }
                        else
                        {
                            var linkEntity = FindLinkEntity(items, entityName);
                            if (linkEntity == null)
                                return sort;

                            linkEntity.AddItem(fetchSort);
                            items = linkEntity.Items;
                        }
                    }

                    return sort.Source;
                }
            }
            else if (node is TopNode top)
            {
                top.Source = MergeNodeDown(top.Source);

                if (!top.Percent && !top.WithTies && top.Source is FetchXmlScan fetchXml)
                {
                    fetchXml.FetchXml.top = top.Top.ToString();
                    return fetchXml;
                }
            }
            else if (node is OffsetFetchNode offset)
            {
                offset.Source = MergeNodeDown(offset.Source);

                if (offset.Source is FetchXmlScan fetchXml)
                {
                    var count = offset.Fetch;
                    var page = offset.Offset / count;

                    if (page * count == offset.Offset)
                    {
                        fetchXml.FetchXml.count = count.ToString();
                        fetchXml.FetchXml.page = (page + 1).ToString();
                        return fetchXml;
                    }
                }
            }
            else if (node is HashMatchAggregateNode aggregate)
            {
                aggregate.Source = MergeNodeDown(aggregate.Source);

                if (aggregate.Source is FetchXmlScan fetchXml)
                {
                    // Check if all the aggregates & groupings can be done in FetchXML. Can only convert them if they can ALL
                    // be handled - if any one needs to be calculated manually, we need to calculate them all
                    foreach (var agg in aggregate.Aggregates)
                    {
                        if (agg.Value.Expression != null && !(agg.Value.Expression is ColumnReferenceExpression))
                            return node;

                        if (agg.Value.Distinct && agg.Value.AggregateType != ExecutionPlan.AggregateType.Count)
                            return node;
                    }

                    fetchXml.FetchXml.aggregate = true;
                    fetchXml.FetchXml.aggregateSpecified = true;
                    fetchXml.FetchXml = fetchXml.FetchXml;

                    var schema = aggregate.Source.GetSchema(Metadata);

                    foreach (var grouping in aggregate.GroupBy)
                    {
                        var colName = grouping.GetColumnName();
                        schema.ContainsColumn(colName, out colName);

                        var attribute = AddAttribute(fetchXml, colName, a => a.groupby == FetchBoolType.@true && a.alias == a.name, out _);
                        attribute.groupby = FetchBoolType.@true;
                        attribute.groupbySpecified = true;
                        attribute.alias = attribute.name;
                    }

                    foreach (var agg in aggregate.Aggregates)
                    {
                        var col = (ColumnReferenceExpression)agg.Value.Expression;
                        var colName = col == null ? schema.PrimaryKey : col.GetColumnName();
                        schema.ContainsColumn(colName, out colName);

                        FetchXml.AggregateType aggregateType;

                        switch (agg.Value.AggregateType)
                        {
                            case ExecutionPlan.AggregateType.Average:
                                aggregateType = FetchXml.AggregateType.avg;
                                break;

                            case ExecutionPlan.AggregateType.Count:
                                aggregateType = FetchXml.AggregateType.countcolumn;
                                break;

                            case ExecutionPlan.AggregateType.CountStar:
                                aggregateType = FetchXml.AggregateType.count;
                                break;

                            case ExecutionPlan.AggregateType.Max:
                                aggregateType = FetchXml.AggregateType.max;
                                break;

                            case ExecutionPlan.AggregateType.Min:
                                aggregateType = FetchXml.AggregateType.min;
                                break;

                            case ExecutionPlan.AggregateType.Sum:
                                aggregateType = FetchXml.AggregateType.sum;
                                break;

                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        var attribute = AddAttribute(fetchXml, colName, a => a.aggregate == aggregateType && a.alias == agg.Key, out _);

                        attribute.aggregateSpecified = true;
                        attribute.alias = agg.Key;
                    }

                    return fetchXml;
                }
            }

            return node;
        }

        private FetchAttributeType AddAttribute(FetchXmlScan fetchXml, string colName, Func<FetchAttributeType,bool> predicate, out bool added)
        {
            var parts = colName.Split('.');
            var entityName = parts[0];
            var attr = new FetchAttributeType { name = parts[1] };

            var entity = fetchXml.FetchXml.Items.OfType<FetchEntityType>().Single();

            if (fetchXml.Alias == entityName)
            {
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
                var linkEntity = FindLinkEntity(entity.Items, entityName);

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

        private bool TranslateCriteria(BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, out filter filter)
        {
            if (!TranslateCriteria(criteria, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var condition, out filter))
                return false;

            if (condition != null)
                filter = new filter { Items = new object[] { condition } };

            return true;
        }

        private bool TranslateCriteria(BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, out condition condition, out filter filter)
        {
            condition = null;
            filter = null;

            if (criteria is BooleanBinaryExpression binary)
            {
                if (!TranslateCriteria(binary.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var lhsCondition, out var lhsFilter))
                    return false;
                if (!TranslateCriteria(binary.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, out var rhsCondition, out var rhsFilter))
                    return false;

                filter = new filter
                {
                    type = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? filterType.and : filterType.or,
                    Items = new []
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
                var expr = comparison.SecondExpression;
                var type = comparison.ComparisonType;

                if (field != null && field2 != null)
                {
                    // The operator is comparing two attributes. This is allowed in join criteria,
                    // but not in filter conditions before version 9.1.0.19251
                    if (!ColumnComparisonAvailable)
                        return false;
                }

                // If we couldn't find the pattern `column = value` or `column = func()`, try looking in the opposite order
                if (field == null && literal == null && func == null)
                {
                    field = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression as Literal;
                    func = comparison.FirstExpression as FunctionCall;
                    expr = comparison.FirstExpression;

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
                if (field == null || (literal == null && func == null && (field2 == null || !ColumnComparisonAvailable) && !IsConstantValueExpression(expr, schema, out literal)))
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

                var meta = Metadata[entityName];

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
                        var targetMetadata = Metadata[value];

                        value = targetMetadata.ObjectTypeCode?.ToString();
                    }

                    if (DateTime.TryParse(value, out var dt) &&
                        dt.Kind != DateTimeKind.Utc &&
                        attribute is DateTimeAttributeMetadata dtAttr &&
                        dtAttr.DateTimeBehavior?.Value != DateTimeBehavior.TimeZoneIndependent &&
                        field.MultiPartIdentifier.Identifiers.Last().Value.Equals(attrName + "utc", StringComparison.OrdinalIgnoreCase))
                    {
                        // Convert the value to UTC if we're filtering on a UTC column
                        if (dt.Kind == DateTimeKind.Unspecified)
                            dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                        else
                            dt = dt.ToUniversalTime();

                        value = dt.ToString("u");
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

            return false;
        }

        private bool IsConstantValueExpression(ScalarExpression expr, NodeSchema schema, out Literal literal)
        {
            literal = null;

            var visitor = new ColumnCollectingVisitor();
            expr.Accept(visitor);

            if (visitor.Columns.Count > 0)
                return false;

            var value = expr.GetValue(null, schema);

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

        private static FetchLinkEntityType FindLinkEntity(object[] items, string alias)
        {
            if (items == null)
                return null;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (linkEntity.alias.Equals(alias, StringComparison.OrdinalIgnoreCase))
                    return linkEntity;

                var childMatch = FindLinkEntity(linkEntity.Items, alias);

                if (childMatch != null)
                    return childMatch;
            }

            return null;
        }
    }
}
