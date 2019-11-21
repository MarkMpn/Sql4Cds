using MarkMpn.Sql4Cds.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;

namespace MarkMpn.Sql4Cds
{
    partial class Sql2FetchXml
    {
        class EntityTable
        {
            public EntityTable(AttributeMetadataCache cache, FetchEntityType entity)
            {
                EntityName = entity.name;
                Entity = entity;
                Metadata = cache[EntityName];
            }

            public EntityTable(AttributeMetadataCache cache, FetchLinkEntityType link)
            {
                EntityName = link.name;
                Alias = link.alias;
                LinkEntity = link;
                Metadata = cache[EntityName];
            }

            public string EntityName { get; set; }
            public string Alias { get; set; }
            public FetchEntityType Entity { get; set; }
            public FetchLinkEntityType LinkEntity { get; set; }
            public IDictionary<int, string> Aggregates { get; } = new Dictionary<int, string>();
            public IDictionary<int, string> Aliases { get; } = new Dictionary<int, string>();
            public EntityMetadata Metadata { get; }

            internal void AddItem(object item)
            {
                if (LinkEntity != null)
                    LinkEntity.Items = Sql2FetchXml.AddItem(LinkEntity.Items, item);
                else
                    Entity.Items = Sql2FetchXml.AddItem(Entity.Items, item);
            }
        }

        public Query[] Convert(string sql, AttributeMetadataCache metadata)
        {
            var queries = new List<Query>();

            var dom = new TSql150Parser(true);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            if (errors.Count > 0)
                throw new QueryParseException(errors[0]);

            var script = (TSqlScript)fragment;

            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    if (statement is SelectStatement select)
                        queries.Add(ConvertSelectStatement(select, metadata));
                    else if (statement is UpdateStatement update)
                        queries.Add(ConvertUpdateStatement(update, metadata));
                    else if (statement is DeleteStatement delete)
                        queries.Add(ConvertDeleteStatement(delete, metadata));
                    else if (statement is InsertStatement insert)
                        queries.Add(ConvertInsertStatement(insert, metadata));
                    else
                        throw new NotSupportedQueryFragmentException("Unsupported statement", statement);
                }
            }

            return queries.ToArray();
        }

        private Query ConvertInsertStatement(InsertStatement insert, AttributeMetadataCache metadata)
        {
            if (insert.OptimizerHints.Count != 0)
                throw new NotSupportedQueryFragmentException("Unhandled INSERT optimizer hints", insert);

            if (insert.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unhandled INSERT WITH clause", insert.WithCtesAndXmlNamespaces);

            if (insert.InsertSpecification.Columns == null)
                throw new NotSupportedQueryFragmentException("Unhandled INSERT without column specification", insert);

            if (insert.InsertSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled INSERT OUTPUT clause", insert.InsertSpecification.OutputClause);

            if (insert.InsertSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled INSERT OUTPUT INTO clause", insert.InsertSpecification.OutputIntoClause);

            if (!(insert.InsertSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException("Unhandled INSERT target", insert.InsertSpecification.Target);

            if (insert.InsertSpecification.InsertSource is ValuesInsertSource values)
                return ConvertInsertValuesStatement(target.SchemaObject.BaseIdentifier.Value, insert.InsertSpecification.Columns, values, metadata);
            else if (insert.InsertSpecification.InsertSource is SelectInsertSource select)
                return ConvertInsertSelectStatement(target.SchemaObject.BaseIdentifier.Value, insert.InsertSpecification.Columns, select, metadata);
            else
                throw new NotSupportedQueryFragmentException("Unhandled INSERT source", insert.InsertSpecification.InsertSource);
        }

        private Query ConvertInsertSelectStatement(string target, IList<ColumnReferenceExpression> columns, SelectInsertSource select, AttributeMetadataCache metadata)
        {
            var qry = new SelectStatement
            {
                QueryExpression = select.Select
            };

            var selectQuery = ConvertSelectStatement(qry, metadata);

            if (columns.Count != selectQuery.ColumnSet.Length)
                throw new NotSupportedQueryFragmentException("Number of columns generated by SELECT does not match number of columns in INSERT", select);

            var query = new InsertSelect
            {
                LogicalName = target,
                FetchXml = selectQuery.FetchXml,
                Mappings = new Dictionary<string, string>(),
                AllPages = selectQuery.FetchXml.page == null && selectQuery.FetchXml.count == null
            };

            for (var i = 0; i < columns.Count; i++)
            {
                query.Mappings[selectQuery.ColumnSet[i]] = columns[i].MultiPartIdentifier.Identifiers.Last().Value;
            }

            return query;
        }

        private Query ConvertInsertValuesStatement(string target, IList<ColumnReferenceExpression> columns, ValuesInsertSource values, AttributeMetadataCache metadata)
        {
            var rowValues = new List<IDictionary<string, object>>();
            var meta = metadata[target];

            foreach (var row in values.RowValues)
            {
                var stringValues = new Dictionary<string, string>();

                if (row.ColumnValues.Count != columns.Count)
                    throw new NotSupportedQueryFragmentException("Number of values does not match number of columns", row);

                for (var i = 0; i < columns.Count; i++)
                {
                    if (!(row.ColumnValues[i] is Literal literal))
                        throw new NotSupportedQueryFragmentException("Only literal values are supported", row.ColumnValues[i]);

                    stringValues[columns[i].MultiPartIdentifier.Identifiers.Last().Value] = literal.Value;
                }

                var rowValue = ConvertAttributeValueTypes(meta, stringValues);
                rowValues.Add(rowValue);
            }

            var query = new InsertValues
            {
                LogicalName = target,
                Values = rowValues.ToArray()
            };

            return query;
        }

        private Query ConvertDeleteStatement(DeleteStatement delete, AttributeMetadataCache metadata)
        {
            if (delete.OptimizerHints.Count != 0)
                throw new NotSupportedQueryFragmentException("Unhandled DELETE optimizer hints", delete);

            if (delete.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unhandled DELETE WITH clause", delete.WithCtesAndXmlNamespaces);

            if (delete.DeleteSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled DELETE OUTPUT clause", delete.DeleteSpecification.OutputClause);

            if (delete.DeleteSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled DELETE OUTPUT INTO clause", delete.DeleteSpecification.OutputIntoClause);

            if (!(delete.DeleteSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException("Unhandled DELETE target table", delete.DeleteSpecification.Target);

            if (delete.DeleteSpecification.FromClause == null)
            {
                delete.DeleteSpecification.FromClause = new FromClause
                {
                    TableReferences =
                    {
                        target
                    }
                };
            }

            var fetch = new FetchXml.FetchType();
            fetch.distinct = true;
            fetch.distinctSpecified = true;
            var tables = HandleFromClause(metadata, delete.DeleteSpecification.FromClause, fetch);
            HandleTopClause(delete.DeleteSpecification.TopRowFilter, fetch, tables);
            HandleWhereClause(delete.DeleteSpecification.WhereClause, fetch, tables);
            
            var table = FindTable(target, tables);
            var meta = metadata[table.EntityName];
            table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });
            var cols = new[] { meta.PrimaryIdAttribute };
            if (table.Entity == null)
                cols[0] = (table.Alias ?? table.EntityName) + "." + cols[0];

            var query = new DeleteQuery
            {
                FetchXml = fetch,
                EntityName = table.EntityName,
                IdColumn = cols[0],
                AllPages = fetch.page == null && fetch.top == null
            };

            return query;
        }

        private UpdateQuery ConvertUpdateStatement(UpdateStatement update, AttributeMetadataCache metadata)
        {
            if (update.OptimizerHints.Count != 0)
                throw new NotSupportedQueryFragmentException("Unhandled UPDATE optimizer hints", update);

            if (update.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unhandled UPDATE WITH clause", update.WithCtesAndXmlNamespaces);

            if (update.UpdateSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled UPDATE OUTPUT clause", update.UpdateSpecification.OutputClause);

            if (update.UpdateSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled UPDATE OUTPUT INTO clause", update.UpdateSpecification.OutputIntoClause);

            if (!(update.UpdateSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException("Unhandled UPDATE target table", update.UpdateSpecification.Target);

            if (update.UpdateSpecification.FromClause == null)
            {
                update.UpdateSpecification.FromClause = new FromClause
                {
                    TableReferences =
                    {
                        target
                    }
                };
            }

            var fetch = new FetchXml.FetchType();
            fetch.distinct = true;
            fetch.distinctSpecified = true;
            var tables = HandleFromClause(metadata, update.UpdateSpecification.FromClause, fetch);
            HandleTopClause(update.UpdateSpecification.TopRowFilter, fetch, tables);
            HandleWhereClause(update.UpdateSpecification.WhereClause, fetch, tables);

            var updates = HandleSetClause(update.UpdateSpecification.SetClauses);

            var table = FindTable(target, tables);
            var meta = metadata[table.EntityName];
            table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });
            var cols = new[] { meta.PrimaryIdAttribute };
            if (table.Entity == null)
                cols[0] = (table.Alias ?? table.EntityName) + "." + cols[0];

            var query = new UpdateQuery
            {
                FetchXml = fetch,
                EntityName = table.EntityName,
                IdColumn = cols[0],
                Updates = ConvertAttributeValueTypes(meta, updates),
                AllPages = fetch.page == null && fetch.top == null
            };

            return query;
        }

        private IDictionary<string, object> ConvertAttributeValueTypes(EntityMetadata metadata, IDictionary<string, string> values)
        {
            return values
                .ToDictionary(kvp => kvp.Key, kvp => ConvertAttributeValueType(metadata, kvp.Key, kvp.Value));
        }

        private object ConvertAttributeValueType(EntityMetadata metadata, string attrName, string value)
        {
            var attr = metadata.Attributes.SingleOrDefault(a => a.LogicalName == attrName);

            if (attr == null)
                throw new NotSupportedException("Unknown attribute " + attrName);

            switch (attr.AttributeType)
            {
                case AttributeTypeCode.BigInt:
                    return Int64.Parse(value);

                case AttributeTypeCode.Boolean:
                    if (value == "0")
                        return false;
                    if (value == "1")
                        return true;
                    throw new FormatException($"Cannot convert value {value} to boolean for attribute {attrName}");

                case AttributeTypeCode.DateTime:
                    return DateTime.Parse(value);

                case AttributeTypeCode.Decimal:
                    return Decimal.Parse(value);

                case AttributeTypeCode.Double:
                    return Double.Parse(value);

                case AttributeTypeCode.Integer:
                    return Int32.Parse(value);

                case AttributeTypeCode.Lookup:
                    var targets = ((LookupAttributeMetadata)attr).Targets;
                    if (targets.Length != 1)
                        throw new NotSupportedException($"Unsupported polymorphic lookup attribute {attrName}");
                    return new EntityReference(targets[0], Guid.Parse(value));

                case AttributeTypeCode.Memo:
                case AttributeTypeCode.String:
                    return value;

                case AttributeTypeCode.Money:
                    return new Money(Decimal.Parse(value));

                case AttributeTypeCode.Picklist:
                case AttributeTypeCode.State:
                case AttributeTypeCode.Status:
                    return new OptionSetValue(Int32.Parse(value));

                default:
                    throw new NotSupportedException($"Unsupport attribute type {attr.AttributeType} for attribute {attrName}");
            }
        }

        private IDictionary<string,string> HandleSetClause(IList<SetClause> setClauses)
        {
            return setClauses
                .Select(set =>
                {
                    if (!(set is AssignmentSetClause assign))
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", set);

                    if (assign.Column == null)
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", assign);

                    if (assign.Column.MultiPartIdentifier.Identifiers.Count > 1)
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", assign.Column);

                    if (!(assign.NewValue is Literal literal))
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", assign);

                    return new { Key = assign.Column.MultiPartIdentifier.Identifiers[0].Value, Value = literal.Value };
                })
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        private SelectQuery ConvertSelectStatement(SelectStatement select, AttributeMetadataCache metadata)
        {
            if (!(select.QueryExpression is QuerySpecification querySpec))
                throw new NotSupportedQueryFragmentException("Unhandled SELECT query expression", select.QueryExpression);

            if (select.ComputeClauses.Count != 0)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT compute clause", select);

            if (select.Into != null)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT INTO clause", select.Into);

            if (select.On != null)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT ON clause", select.On);

            if (select.OptimizerHints.Count != 0)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT optimizer hints", select);

            if (select.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT WITH clause", select.WithCtesAndXmlNamespaces);

            if (querySpec.ForClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT FOR clause", querySpec.ForClause);

            if (querySpec.HavingClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT HAVING clause", querySpec.HavingClause);

            if (querySpec.FromClause == null)
                throw new NotSupportedQueryFragmentException("No source entity specified", querySpec);

            var fetch = new FetchXml.FetchType();
            var tables = HandleFromClause(metadata, querySpec.FromClause, fetch);
            HandleSelectClause(metadata, querySpec, fetch, tables, out var columns);
            HandleTopClause(querySpec.TopRowFilter, fetch, tables);
            HandleOffsetClause(querySpec, fetch, tables);
            HandleWhereClause(querySpec.WhereClause, fetch, tables);
            HandleGroupByClause(querySpec, fetch, tables, columns);
            HandleOrderByClause(querySpec, fetch, tables, columns);
            HandleDistinctClause(querySpec, fetch, tables);
            
            return new SelectQuery
            {
                FetchXml = fetch,
                ColumnSet = columns,
                AllPages = fetch.page == null && fetch.count == null
            };
        }

        private void HandleGroupByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns)
        {
            if (querySpec.GroupByClause == null)
                return;

            if (querySpec.GroupByClause.All == true)
                throw new NotSupportedQueryFragmentException("Unhandled GROUP BY ALL clause", querySpec.GroupByClause);

            if (querySpec.GroupByClause.GroupByOption != GroupByOption.None)
                throw new NotSupportedQueryFragmentException("Unhandled GROUP BY option", querySpec.GroupByClause);

            fetch.aggregate = true;
            fetch.aggregateSpecified = true;

            foreach (var group in querySpec.GroupByClause.GroupingSpecifications)
            {
                if (!(group is ExpressionGroupingSpecification exprGroup))
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY clause", group);

                if (!(exprGroup.Expression is ColumnReferenceExpression col))
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY clause", exprGroup.Expression);

                GetColumnTableAlias(col, tables, out var table);

                if (table == null)
                    throw new NotSupportedQueryFragmentException("Unknown table", col);

                var attr = (table.Entity?.Items ?? table.LinkEntity.Items)
                    .OfType<FetchAttributeType>()
                    .Where(a => a.name == col.MultiPartIdentifier.Identifiers.Last().Value)
                    .SingleOrDefault();

                if (attr == null)
                {
                    attr = new FetchAttributeType { name = col.MultiPartIdentifier.Identifiers.Last().Value };
                    table.AddItem(attr);
                }

                if (attr.alias == null)
                {
                    attr.alias = attr.name;

                    for (var i = 0; i < columns.Length; i++)
                    {
                        if (columns[i].Equals($"{table.Alias ?? table.EntityName}.{attr.name}", StringComparison.OrdinalIgnoreCase))
                            columns[i] = attr.name;
                    }
                }

                attr.groupby = FetchBoolType.@true;
                attr.groupbySpecified = true;
            }
        }

        private void HandleDistinctClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (querySpec.UniqueRowFilter == UniqueRowFilter.Distinct)
            {
                fetch.distinct = true;
                fetch.distinctSpecified = true;
            }
        }

        private void HandleOffsetClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (querySpec.OffsetClause == null)
                return;

            if (!(querySpec.OffsetClause.OffsetExpression is IntegerLiteral offset))
                throw new NotSupportedQueryFragmentException("Unhandled OFFSET clause offset expression", querySpec.OffsetClause.OffsetExpression);

            if (!(querySpec.OffsetClause.FetchExpression is IntegerLiteral fetchCount))
                throw new NotSupportedQueryFragmentException("Unhandled OFFSET clause fetch expression", querySpec.OffsetClause.FetchExpression);

            var pageSize = Int32.Parse(fetchCount.Value);
            var pageNumber = (decimal)Int32.Parse(offset.Value) / pageSize + 1;

            if (pageNumber != (int)pageNumber)
                throw new NotSupportedQueryFragmentException("Offset must be an integer multiple of fetch", querySpec.OffsetClause);

            fetch.count = pageSize.ToString();
            fetch.page = pageNumber.ToString();
        }

        private void HandleTopClause(TopRowFilter top, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (top == null)
                return;

            if (top.Percent)
                throw new NotSupportedQueryFragmentException("Unhandled TOP PERCENT clause", top);

            if (top.WithTies)
                throw new NotSupportedQueryFragmentException("Unhandled TOP WITH TIES clause", top);

            if (!(top.Expression is IntegerLiteral topLiteral))
                throw new NotSupportedQueryFragmentException("Unhandled TOP expression", top.Expression);

            fetch.top = topLiteral.Value;
        }

        private void HandleOrderByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns)
        {
            if (querySpec.OrderByClause == null)
                return;

            foreach (var sort in querySpec.OrderByClause.OrderByElements)
            {
                if (!(sort.Expression is ColumnReferenceExpression col))
                {
                    if (sort.Expression is IntegerLiteral colIndex)
                    {
                        var colName = columns[Int32.Parse(colIndex.Value) - 1];
                        col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

                        foreach (var part in colName.Split('.'))
                            col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });
                    }
                    else
                    {
                        throw new NotSupportedQueryFragmentException("Unsupported ORDER BY clause", sort.Expression);
                    }
                }

                var orderTable = GetColumnTableAlias(col, tables, out _);

                if (orderTable != null)
                    throw new NotSupportedQueryFragmentException("Unsupported ORDER BY on linked table", sort.Expression);

                var order = new FetchOrderType
                {
                    attribute = GetColumnAttribute(col),
                    descending = sort.SortOrder == SortOrder.Descending
                };

                // For aggregate queries, ordering must be done on aliases not attributes
                if (fetch.aggregate)
                {
                    var attr = tables[0].Entity.Items
                        .OfType<FetchAttributeType>()
                        .SingleOrDefault(a => a.alias == order.attribute);

                    if (attr == null)
                    {
                        attr = tables[0].Entity.Items
                            .OfType<FetchAttributeType>()
                            .SingleOrDefault(a => a.alias == null && a.name == order.attribute);
                    }

                    if (attr == null)
                    {
                        attr = new FetchAttributeType { name = order.attribute };
                        tables[0].AddItem(attr);
                    }

                    if (attr.alias == null)
                        attr.alias = order.attribute;

                    order.alias = attr.alias;
                    order.attribute = null;
                }
                
                tables[0].AddItem(order);
            }
        }

        private void HandleWhereClause(WhereClause where, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (where == null)
                return;

            if (where.Cursor != null)
                throw new NotSupportedQueryFragmentException("Unhandled WHERE clause", where.Cursor);

            var filter = new filter
            {
                type = (filterType)2
            };

            tables[0].AddItem(filter);

            ColumnReferenceExpression col1 = null;
            ColumnReferenceExpression col2 = null;
            HandleFilter(where.SearchCondition, filter, tables, tables[0], false, ref col1, ref col2);

            if (col1 != null || col2 != null)
                throw new NotSupportedQueryFragmentException("Unsupported comparison", col1);

            if (filter.type == (filterType)2)
                filter.type = filterType.and;
        }

        private void HandleFilter(BooleanExpression searchCondition, filter criteria, List<EntityTable> tables, EntityTable targetTable, bool inOr, ref ColumnReferenceExpression col1, ref ColumnReferenceExpression col2)
        {
            if (searchCondition is BooleanComparisonExpression comparison)
            {
                var field = comparison.FirstExpression as ColumnReferenceExpression;
                var literal = comparison.SecondExpression as Literal;
                var func = comparison.SecondExpression as FunctionCall;
                var field2 = comparison.SecondExpression as ColumnReferenceExpression;

                if (field != null && field2 != null)
                {
                    if (col1 == null && col2 == null)
                    {
                        if (inOr)
                            throw new NotSupportedQueryFragmentException("Cannot combine join criteria with OR", comparison);

                        col1 = field;
                        col2 = field2;
                        return;
                    }

                    throw new NotSupportedQueryFragmentException("Unsupported comparison", comparison);
                }

                if (field == null && literal == null && func == null)
                {
                    field = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression as Literal;
                    func = comparison.FirstExpression as FunctionCall;
                }

                if (field == null || (literal == null && func == null))
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", comparison);

                @operator op;

                switch (comparison.ComparisonType)
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
                
                object value = null;

                if (literal != null)
                {
                    switch (literal.LiteralType)
                    {
                        case LiteralType.Integer:
                            value = Int32.Parse(literal.Value);
                            break;

                        case LiteralType.Money:
                            value = Decimal.Parse(literal.Value);
                            break;

                        case LiteralType.Numeric:
                        case LiteralType.Real:
                            value = Double.Parse(literal.Value);
                            break;

                        case LiteralType.String:
                            value = literal.Value;
                            break;

                        default:
                            throw new NotSupportedQueryFragmentException("Unsupported literal type", literal);
                    }
                }
                else if (op == @operator.eq)
                {
                    op = (@operator) Enum.Parse(typeof(@operator), func.FunctionName.Value);

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

                    if (func.Parameters.Count > 1)
                        throw new NotSupportedQueryFragmentException("Unsupported number of function parameters", func);

                    if (func.Parameters.Count == 1)
                    {
                        if (!(func.Parameters[0] is Literal paramLiteral))
                            throw new NotSupportedQueryFragmentException("Unsupported function parameter", func.Parameters[0]);

                        value = paramLiteral.Value;
                    }
                }
                else
                {
                    throw new NotSupportedQueryFragmentException("Unsupported function use. Only <field> = <func>(<param>) usage is supported", comparison);
                }

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);

                if (entityTable == targetTable)
                    entityName = null;
                
                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = entityName,
                    attribute = GetColumnAttribute(field),
                    @operator = op,
                    value = value?.ToString()
                });
            }
            else if (searchCondition is BooleanBinaryExpression binary)
            {
                var op = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? filterType.and : filterType.or;

                if (op != criteria.type && criteria.type != (filterType) 2)
                {
                    var subFilter = new filter { type = op };
                    criteria.Items = AddItem(criteria.Items, subFilter);
                    criteria = subFilter;
                }
                else
                {
                    criteria.type = op;
                }

                HandleFilter(binary.FirstExpression, criteria, tables, targetTable, inOr || op == filterType.or, ref col1, ref col2);
                HandleFilter(binary.SecondExpression, criteria, tables, targetTable, inOr || op == filterType.or, ref col1, ref col2);
            }
            else if (searchCondition is BooleanParenthesisExpression paren)
            {
                var subFilter = new filter { type = (filterType)2 };
                criteria.Items = AddItem(criteria.Items, subFilter);
                criteria = subFilter;

                HandleFilter(paren.Expression, criteria, tables, targetTable, inOr, ref col1, ref col2);

                if (subFilter.type == (filterType)2)
                    subFilter.type = filterType.and;
            }
            else if (searchCondition is BooleanIsNullExpression isNull)
            {
                var field = isNull.Expression as ColumnReferenceExpression;

                if (field == null)
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", isNull.Expression);

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = field.MultiPartIdentifier.Identifiers.Count == 2 ? field.MultiPartIdentifier.Identifiers[0].Value : null,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = isNull.IsNot ? @operator.notnull : @operator.@null
                });
            }
            else if (searchCondition is LikePredicate like)
            {
                var field = like.FirstExpression as ColumnReferenceExpression;

                if (field == null)
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", like.FirstExpression);

                var value = like.SecondExpression as StringLiteral;

                if (value == null)
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", like.SecondExpression);

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = GetColumnTableAlias(field, tables, out _),
                    attribute = GetColumnAttribute(field),
                    @operator = like.NotDefined ? @operator.notlike : @operator.like,
                    value = value.Value
                });
            }
            else if (searchCondition is InPredicate @in)
            {
                var field = @in.Expression as ColumnReferenceExpression;

                if (field == null)
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", @in.Expression);

                if (@in.Subquery != null)
                    throw new NotSupportedQueryFragmentException("Unsupported subquery, rewrite query as join", @in.Subquery);

                var condition = new condition
                {
                    entityname = field.MultiPartIdentifier.Identifiers.Count == 2 ? field.MultiPartIdentifier.Identifiers[0].Value : null,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = @in.NotDefined ? @operator.notin : @operator.@in
                };
                
                condition.Items = @in.Values
                    .Select(v =>
                    {
                        if (!(v is Literal literal))
                            throw new NotSupportedQueryFragmentException("Unsupported comparison", v);

                        return new conditionValue
                        {
                            Value = literal.Value
                        };
                    })
                    .ToArray();
            }
            else
            {
                throw new NotSupportedQueryFragmentException("Unhandled WHERE clause", searchCondition);
            }
        }

        private void HandleSelectClause(AttributeMetadataCache metadata, QuerySpecification select, FetchXml.FetchType fetch, List<EntityTable> tables, out string[] columns)
        {
            var cols = new List<string>();
            
            foreach (var field in select.SelectElements)
            {
                if (field is SelectStarExpression star)
                {
                    var starTables = tables;

                    if (star.Qualifier != null)
                        starTables = new List<EntityTable> { FindTable(star.Qualifier.Identifiers.Last().Value, tables, field) };

                    foreach (var starTable in starTables)
                    {
                        starTable.AddItem(new allattributes());

                        var meta = metadata[starTable.EntityName];

                        foreach (var attr in meta.Attributes.Where(a => a.IsValidForRead == true))
                        {
                            if (starTable.LinkEntity == null)
                                cols.Add(attr.LogicalName);
                            else
                                cols.Add((starTable.Alias ?? starTable.EntityName) + "." + attr.LogicalName);
                        }
                    }
                }
                else if (field is SelectScalarExpression scalar)
                {
                    var expr = scalar.Expression;
                    var func = expr as FunctionCall;

                    if (func != null)
                    {
                        if (func.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException("Unhandled function", func);

                        if (!(func.Parameters[0] is ColumnReferenceExpression colParam))
                            throw new NotSupportedQueryFragmentException("Unhandled function parameter", func.Parameters[0]);

                        expr = colParam;
                    }

                    if (expr is ColumnReferenceExpression col)
                    {
                        string attrName;
                        if (col.ColumnType == ColumnType.Wildcard)
                            attrName = metadata[tables[0].EntityName].PrimaryIdAttribute;
                        else
                            attrName = col.MultiPartIdentifier.Identifiers.Last().Value;

                        EntityTable table;

                        if (col.ColumnType == ColumnType.Wildcard)
                            table = tables[0];
                        else
                            GetColumnTableAlias(col, tables, out table);

                        var attr = new FetchAttributeType { name = attrName };
                        table.AddItem(attr);

                        var alias = scalar.ColumnName?.Identifier?.Value;

                        if (func != null)
                        {
                            switch (func.FunctionName.Value)
                            {
                                case "count":
                                    attr.aggregate = col.ColumnType == ColumnType.Wildcard ? AggregateType.count : AggregateType.countcolumn;
                                    break;

                                case "avg":
                                case "min":
                                case "max":
                                case "sum":
                                    attr.aggregate = (AggregateType) Enum.Parse(typeof(AggregateType), func.FunctionName.Value);
                                    break;

                                default:
                                    throw new NotSupportedQueryFragmentException("Unhandled function", func);
                            }

                            attr.aggregateSpecified = true;
                            fetch.aggregate = true;
                            fetch.aggregateSpecified = true;

                            if (func.UniqueRowFilter == UniqueRowFilter.Distinct)
                            {
                                attr.distinct = FetchBoolType.@true;
                                attr.distinctSpecified = true;
                            }

                            if (alias == null)
                            {
                                alias = $"{attrName.Replace(".", "_")}_{attr.aggregate}";
                                var counter = 1;

                                while (cols.Contains(alias))
                                {
                                    counter++;
                                    alias = $"{attrName.Replace(".", "_")}_{attr.aggregate}_{counter}";
                                }
                            }
                        }

                        attr.alias = alias;

                        if (alias == null)
                            cols.Add((table.LinkEntity == null ? "" : ((table.Alias ?? table.EntityName) + ".")) + attr.name);
                        else
                            cols.Add(alias);
                    }
                    else
                    {
                        throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", scalar.Expression);
                    }
                }
                else
                {
                    throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", field);
                }
            }
            
            columns = cols.ToArray();
        }

        private string GetAttributeName(ColumnReferenceExpression col, List<EntityTable> tables)
        {
            if (col.MultiPartIdentifier.Identifiers.Count > 2)
                throw new NotSupportedQueryFragmentException("Unhandled column reference format", col);

            var attr = col.MultiPartIdentifier.Identifiers.Last().Value;

            if (col.MultiPartIdentifier.Identifiers.Count == 2 && !col.MultiPartIdentifier.Identifiers[0].Value.Equals(tables[0].Alias, StringComparison.OrdinalIgnoreCase))
                attr = col.MultiPartIdentifier.Identifiers[0].Value + "." + attr;

            return attr;
        }

        private List<EntityTable> HandleFromClause(AttributeMetadataCache metadata, FromClause from, FetchXml.FetchType fetch)
        {
            if (from.TableReferences.Count != 1)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT FROM clause - only single table or qualified joins are supported", from);

            var tables = new List<EntityTable>();

            HandleFromClause(metadata, from.TableReferences[0], fetch, tables);

            return tables;
        }

        private void HandleFromClause(AttributeMetadataCache metadata, TableReference tableReference, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (tableReference is NamedTableReference namedTable)
            {
                var table = FindTable(namedTable, tables);

                if (table == null && fetch.Items == null)
                {
                    var entity = new FetchEntityType
                    {
                        name = namedTable.SchemaObject.BaseIdentifier.Value
                    };
                    fetch.Items = new object[] { entity };

                    try
                    {
                        table = new EntityTable(metadata, entity) { Alias = namedTable.Alias?.Value };
                    }
                    catch (FaultException ex)
                    {
                        throw new NotSupportedQueryFragmentException(ex.Message, tableReference);
                    }

                    tables.Add(table);

                    foreach (var hint in namedTable.TableHints)
                    {
                        if (hint.HintKind == TableHintKind.NoLock)
                            fetch.nolock = true;
                        else
                            throw new NotSupportedQueryFragmentException("Unsupported table hint", hint);
                    }
                }
            }
            else if (tableReference is QualifiedJoin join)
            {
                if (join.JoinHint != JoinHint.None)
                    throw new NotSupportedQueryFragmentException("Unsupported join hint", join);

                if (!(join.SecondTableReference is NamedTableReference table2))
                    throw new NotSupportedQueryFragmentException("Unsupported join table", join.SecondTableReference);

                HandleFromClause(metadata, join.FirstTableReference, fetch, tables);

                var link = new FetchLinkEntityType
                {
                    name = table2.SchemaObject.BaseIdentifier.Value,
                    alias = table2.Alias?.Value ?? table2.SchemaObject.BaseIdentifier.Value
                };

                EntityTable linkTable;

                try
                {
                    linkTable = new EntityTable(metadata, link);
                    tables.Add(linkTable);
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(ex.Message, table2);
                }
                
                var filter = new filter
                {
                    type = (filterType)2
                };
                
                ColumnReferenceExpression col1 = null;
                ColumnReferenceExpression col2 = null;
                HandleFilter(join.SearchCondition, filter, tables, linkTable, false, ref col1, ref col2);

                if (col1 == null || col2 == null)
                    throw new NotSupportedQueryFragmentException("Missing join condition", join.SearchCondition);

                if (filter.type != (filterType)2)
                    linkTable.AddItem(filter);

                switch (join.QualifiedJoinType)
                {
                    case QualifiedJoinType.Inner:
                        link.linktype = "inner";
                        break;

                    case QualifiedJoinType.LeftOuter:
                        link.linktype = "outer";
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unsupported join type", join);
                }

                ColumnReferenceExpression linkFromAttribute;
                ColumnReferenceExpression linkToAttribute;

                GetColumnTableAlias(col1, tables, out var lhs);
                GetColumnTableAlias(col2, tables, out var rhs);

                if (lhs == null || rhs == null)
                    throw new NotSupportedQueryFragmentException("Join condition does not reference previous table", join.SearchCondition);

                if (rhs == linkTable)
                {
                    linkFromAttribute = col1;
                    linkToAttribute = col2;
                }
                else if (lhs == linkTable)
                {
                    linkFromAttribute = col2;
                    linkToAttribute = col1;

                    lhs = rhs;
                    rhs = linkTable;
                }
                else
                {
                    throw new NotSupportedQueryFragmentException("Join condition does not reference joined table", join.SearchCondition);
                }

                link.from = linkToAttribute.MultiPartIdentifier.Identifiers.Last().Value;
                link.to = linkFromAttribute.MultiPartIdentifier.Identifiers.Last().Value;

                lhs.AddItem(link);
            }
            else
            {
                throw new NotSupportedQueryFragmentException("Unhandled SELECT FROM clause", tableReference);
            }
        }

        private EntityTable FindTable(NamedTableReference namedTable, List<EntityTable> tables)
        {
            if (namedTable.Alias != null)
            {
                var aliasedTable = tables.SingleOrDefault(t => t.Alias.Equals(namedTable.Alias.Value, StringComparison.OrdinalIgnoreCase));

                if (aliasedTable == null)
                    return null;

                if (!aliasedTable.EntityName.Equals(namedTable.SchemaObject.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedQueryFragmentException("Duplicate table alias", namedTable);

                return aliasedTable;
            }

            var table = tables.SingleOrDefault(t => t.Alias != null && t.Alias.Equals(namedTable.SchemaObject.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase));

            if (table == null)
                table = tables.SingleOrDefault(t => t.Alias == null && t.EntityName.Equals(namedTable.SchemaObject.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase));

            return table;
        }

        private EntityTable FindTable(string name, List<EntityTable> tables, TSqlFragment fragment)
        {
            var matches = tables
                .Where(t => t.Alias != null && t.Alias.Equals(name, StringComparison.OrdinalIgnoreCase) || t.Alias == null && t.EntityName.Equals(name, StringComparison.OrdinalIgnoreCase))
                .ToArray();

            if (matches.Length == 0)
                return null;

            if (matches.Length == 1)
                return matches[0];

            throw new NotSupportedQueryFragmentException("Ambiguous identifier " + name, fragment);
        }

        private string GetColumnTableAlias(ColumnReferenceExpression col, List<EntityTable> tables, out EntityTable table)
        {
            if (col.MultiPartIdentifier.Identifiers.Count > 2)
                throw new NotSupportedQueryFragmentException("Unsupported column reference", col);

            if (col.MultiPartIdentifier.Identifiers.Count == 2)
            {
                var alias = col.MultiPartIdentifier.Identifiers[0].Value;

                if (alias.Equals(tables[0].Alias ?? tables[0].EntityName, StringComparison.OrdinalIgnoreCase))
                {
                    table = tables[0];
                    return null;
                }

                table = tables.SingleOrDefault(t => t.Alias != null && t.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase));
                if (table == null)
                    table = tables.SingleOrDefault(t => t.Alias == null && t.EntityName.Equals(alias, StringComparison.OrdinalIgnoreCase));

                return alias;
            }

            // If no table is explicitly specified, check in the metadata for each available table
            var possibleEntities = tables
                .Where(t => t.Metadata.Attributes.Any(attr => attr.LogicalName.Equals(col.MultiPartIdentifier.Identifiers[0].Value, StringComparison.OrdinalIgnoreCase)))
                .ToArray();

            if (possibleEntities.Length == 0)
            {
                // If we couldn't find a match in the metadata, we might have an alias we can use instead
                possibleEntities = tables
                    .Where(t => (t.Entity?.Items ?? t.LinkEntity?.Items).OfType<FetchAttributeType>().Any(attr => attr.alias?.Equals(col.MultiPartIdentifier.Identifiers[0].Value, StringComparison.OrdinalIgnoreCase) == true))
                    .ToArray();
            }

            if (possibleEntities.Length == 0)
                throw new NotSupportedQueryFragmentException("Unknown attribute", col);

            if (possibleEntities.Length > 1)
                throw new NotSupportedQueryFragmentException("Ambiguous attribute", col);

            table = possibleEntities[0];

            if (possibleEntities[0] == tables[0])
                return null;

            return possibleEntities[0].Alias ?? possibleEntities[0].EntityName;
        }

        private string GetColumnAttribute(ColumnReferenceExpression col)
        {
            return col.MultiPartIdentifier.Identifiers.Last().Value;
        }

        private static object[] AddItem(object[] items, object item)
        {
            if (items == null)
                return new[] { item };

            var list = new List<object>(items);
            list.Add(item);
            return list.ToArray();
        }
    }
}
