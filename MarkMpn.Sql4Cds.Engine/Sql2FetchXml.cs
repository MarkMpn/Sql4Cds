using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.ServiceModel;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Converts a SQL query to the corresponding FetchXML query
    /// </summary>
    public class Sql2FetchXml
    {
        /// <summary>
        /// Represents a table from the SQL query and the corresponding entity or link-entity in the FetchXML conversion
        /// </summary>
        class EntityTable
        {
            /// <summary>
            /// Creates a new <see cref="EntityTable"/> based on the top-level entity in the query
            /// </summary>
            /// <param name="cache">The metadata cache to use</param>
            /// <param name="entity">The entity object in the FetchXML query</param>
            public EntityTable(IAttributeMetadataCache cache, FetchEntityType entity)
            {
                EntityName = entity.name;
                Entity = entity;
                Metadata = cache[EntityName];
            }

            /// <summary>
            /// Creates a new <see cref="EntityTable"/> based on a link-entity in the query
            /// </summary>
            /// <param name="cache">The metadata cache to use</param>
            /// <param name="link">The link-entity object in the FetchXML query</param>
            public EntityTable(IAttributeMetadataCache cache, FetchLinkEntityType link)
            {
                EntityName = link.name;
                Alias = link.alias;
                LinkEntity = link;
                Metadata = cache[EntityName];
            }

            /// <summary>
            /// The logical name of the entity
            /// </summary>
            public string EntityName { get; set; }

            /// <summary>
            /// The alias of the entity
            /// </summary>
            public string Alias { get; set; }

            /// <summary>
            /// The entity from the FetchXML query
            /// </summary>
            public FetchEntityType Entity { get; set; }

            /// <summary>
            /// The link-entity from the FetchXML query
            /// </summary>
            public FetchLinkEntityType LinkEntity { get; set; }

            /// <summary>
            /// Returns the metadata for this entity
            /// </summary>
            public EntityMetadata Metadata { get; }

            /// <summary>
            /// Adds a child to the entity or link-entity
            /// </summary>
            /// <param name="item">The item to add to the entity or link-entity</param>
            internal void AddItem(object item)
            {
                if (LinkEntity != null)
                    LinkEntity.Items = Sql2FetchXml.AddItem(LinkEntity.Items, item);
                else
                    Entity.Items = Sql2FetchXml.AddItem(Entity.Items, item);
            }

            /// <summary>
            /// Removes any items from the entity or link-entity that match a predicate
            /// </summary>
            /// <param name="predicate">The predicate to identify the items to remove</param>
            internal void RemoveItems(Func<object,bool> predicate)
            {
                if (LinkEntity?.Items != null)
                    LinkEntity.Items = LinkEntity.Items.Where(i => !predicate(i)).ToArray();
                else if (Entity?.Items != null)
                    Entity.Items = Entity.Items.Where(i => !predicate(i)).ToArray();
            }

            /// <summary>
            /// Checks if the entity or link-entity contains any items that match a predicate
            /// </summary>
            /// <param name="predicate">The predicate to search for</param>
            /// <returns><c>true</c> if there is a child item that matches the <paramref name="predicate"/>, or <c>false</c> otherwise</returns>
            internal bool Contains(Func<object, bool> predicate)
            {
                if (LinkEntity?.Items != null)
                    return LinkEntity.Items.Any(predicate);
                else if (Entity?.Items != null)
                    return Entity.Items.Where(predicate).Any();

                return false;
            }

            /// <summary>
            /// Sorts the elements within the entity or link-entity to put them in an order people expect based on standard online samples
            /// </summary>
            internal void Sort()
            {
                if (LinkEntity?.Items != null)
                {
                    LinkEntity.Items.StableSort(new FetchXmlElementComparer());
                    LinkEntity.Items = RemoveEmptyFilters(LinkEntity.Items);
                }
                else if (Entity?.Items != null)
                {
                    Entity.Items.StableSort(new FetchXmlElementComparer());
                    Entity.Items = RemoveEmptyFilters(Entity.Items);
                }
            }

            private object[] RemoveEmptyFilters(object[] items)
            {
                if (items == null)
                    return null;

                var keep = new List<object>();

                foreach (var item in items)
                {
                    if (item is filter f)
                    {
                        f.Items = RemoveEmptyFilters(f.Items);

                        if (f.Items == null || f.Items.Length == 0)
                            continue;
                    }

                    keep.Add(item);
                }

                return keep.ToArray();
            }
        }

        private static readonly ParameterExpression _param = Expression.Parameter(typeof(Entity), "entity");

        /// <summary>
        /// Creates a new <see cref="Sql2FetchXml"/> converter
        /// </summary>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="quotedIdentifiers">Indicates if the SQL should be parsed using quoted identifiers</param>
        public Sql2FetchXml(IAttributeMetadataCache metadata, bool quotedIdentifiers)
        {
            Metadata = metadata;
            QuotedIdentifiers = quotedIdentifiers;
        }

        /// <summary>
        /// Returns the metadata cache that will be used by this conversion
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// Returns or sets a value indicating if SQL will be parsed using quoted identifiers
        /// </summary>
        public bool QuotedIdentifiers { get; set; }

        /// <summary>
        /// Parses a SQL batch and returns the queries identified in it
        /// </summary>
        /// <param name="sql">The SQL batch to parse</param>
        /// <returns>An array of queries that can be run against CDS, converted from the supplied <paramref name="sql"/></returns>
        /// <remarks>
        /// If an error is encountered when parsing the SQL, a <see cref="QueryParseException"/> is thrown.
        /// 
        /// If the SQL can be parsed correctly but contains elements that aren't supported in the conversion to FetchXML,
        /// a <see cref="NotSupportedQueryFragmentException"/> is thrown.
        /// </remarks>
        public Query[] Convert(string sql)
        {
            var queries = new List<Query>();

            // Parse the SQL DOM
            var dom = new TSql150Parser(QuotedIdentifiers);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            // Check if there were any parse errors
            if (errors.Count > 0)
                throw new QueryParseException(errors[0]);

            var script = (TSqlScript)fragment;

            // Convert each statement in turn to the appropriate query type
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    if (statement is SelectStatement select)
                        queries.Add(ConvertSelectStatement(select));
                    else if (statement is UpdateStatement update)
                        queries.Add(ConvertUpdateStatement(update));
                    else if (statement is DeleteStatement delete)
                        queries.Add(ConvertDeleteStatement(delete));
                    else if (statement is InsertStatement insert)
                        queries.Add(ConvertInsertStatement(insert));
                    else
                        throw new NotSupportedQueryFragmentException("Unsupported statement", statement);
                }
            }

            return queries.ToArray();
        }

        /// <summary>
        /// Convert an INSERT statement from SQL
        /// </summary>
        /// <param name="insert">The parsed INSERT statement</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertInsertStatement(InsertStatement insert)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
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

            // Check if we are inserting constant values or the results of a SELECT statement and perform the appropriate conversion
            if (insert.InsertSpecification.InsertSource is ValuesInsertSource values)
                return ConvertInsertValuesStatement(target.SchemaObject.BaseIdentifier.Value, insert.InsertSpecification.Columns, values);
            else if (insert.InsertSpecification.InsertSource is SelectInsertSource select)
                return ConvertInsertSelectStatement(target.SchemaObject.BaseIdentifier.Value, insert.InsertSpecification.Columns, select);
            else
                throw new NotSupportedQueryFragmentException("Unhandled INSERT source", insert.InsertSpecification.InsertSource);
        }

        /// <summary>
        /// Convert an INSERT INTO ... SELECT ... query
        /// </summary>
        /// <param name="target">The entity to insert the results into</param>
        /// <param name="columns">The list of columns within the <paramref name="target"/> entity to populate with the results of the <paramref name="select"/> query</param>
        /// <param name="select">The SELECT query that provides the values to insert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertInsertSelectStatement(string target, IList<ColumnReferenceExpression> columns, SelectInsertSource select)
        {
            // Reuse the standard SELECT query conversion for the data source
            var qry = new SelectStatement
            {
                QueryExpression = select.Select
            };

            var selectQuery = ConvertSelectStatement(qry);

            // Check that the number of columns for the source query and target columns match
            if (columns.Count != selectQuery.ColumnSet.Length)
                throw new NotSupportedQueryFragmentException("Number of columns generated by SELECT does not match number of columns in INSERT", select);

            // Populate the final query based on the converted SELECT query
            var query = new InsertSelect
            {
                LogicalName = target,
                FetchXml = selectQuery.FetchXml,
                Mappings = new Dictionary<string, string>(),
                AllPages = selectQuery.FetchXml.page == null && selectQuery.FetchXml.count == null
            };

            for (var i = 0; i < columns.Count; i++)
                query.Mappings[selectQuery.ColumnSet[i]] = columns[i].MultiPartIdentifier.Identifiers.Last().Value;

            return query;
        }

        /// <summary>
        /// Convert an INSERT INTO ... VALUES ... query
        /// </summary>
        /// <param name="target">The entity to insert the values into</param>
        /// <param name="columns">The list of columns within the <paramref name="target"/> entity to populate with the supplied <paramref name="values"/></param>
        /// <param name="values">The values to insert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertInsertValuesStatement(string target, IList<ColumnReferenceExpression> columns, ValuesInsertSource values)
        {
            // Get the metadata for the target entity
            var rowValues = new List<IDictionary<string, object>>();
            var meta = Metadata[target];

            // Convert the supplied values to the appropriate type for the attribute it is to be inserted into
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

            // Return the final query
            var query = new InsertValues
            {
                LogicalName = target,
                Values = rowValues.ToArray()
            };

            return query;
        }

        /// <summary>
        /// Converts a DELETE query
        /// </summary>
        /// <param name="delete">The SQL query to convert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertDeleteStatement(DeleteStatement delete)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
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

            // Get the entity that the records should be deleted from
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

            // Convert the FROM, TOP and WHERE clauses from the query to identify the records to delete.
            // Each record can only be deleted once, so apply the DISTINCT option as well
            var fetch = new FetchXml.FetchType
            {
                distinct = true,
                distinctSpecified = true
            };
            var tables = HandleFromClause(delete.DeleteSpecification.FromClause, fetch);
            HandleTopClause(delete.DeleteSpecification.TopRowFilter, fetch);
            HandleWhereClause(delete.DeleteSpecification.WhereClause, tables, false, out var postFilter);
            
            // To delete a record we need the primary key field of the target entity
            var table = FindTable(target, tables);
            var meta = Metadata[table.EntityName];
            table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });
            var cols = new[] { meta.PrimaryIdAttribute };
            if (table.Entity == null)
                cols[0] = (table.Alias ?? table.EntityName) + "." + cols[0];

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var t in tables)
                t.Sort();

            // Return the final query
            var query = new DeleteQuery
            {
                FetchXml = fetch,
                PostFilter = postFilter,
                EntityName = table.EntityName,
                IdColumn = cols[0],
                AllPages = fetch.page == null && fetch.top == null
            };

            if (postFilter != null && fetch.top != null)
            {
                query.PostTop = Int32.Parse(fetch.top);
                fetch.top = null;
            }

            return query;
        }

        /// <summary>
        /// Converts an UPDATE query
        /// </summary>
        /// <param name="update">The SQL query to convert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private UpdateQuery ConvertUpdateStatement(UpdateStatement update)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
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

            // Get the entity that the records should be updated in
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

            // Convert the FROM, TOP and WHERE clauses from the query to identify the records to update.
            // Each record can only be updated once, so apply the DISTINCT option as well
            var fetch = new FetchXml.FetchType
            {
                distinct = true,
                distinctSpecified = true
            };
            var tables = HandleFromClause(update.UpdateSpecification.FromClause, fetch);
            HandleTopClause(update.UpdateSpecification.TopRowFilter, fetch);
            HandleWhereClause(update.UpdateSpecification.WhereClause, tables, false, out var postFilter);

            var table = FindTable(target, tables);
            var meta = Metadata[table.EntityName];

            // Get the details of what fields should be updated to what
            var updates = HandleSetClause(update.UpdateSpecification.SetClauses, tables, meta);

            // To update a record we need the primary key field of the target entity
            table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });
            var cols = new[] { meta.PrimaryIdAttribute };
            if (table.Entity == null)
                cols[0] = (table.Alias ?? table.EntityName) + "." + cols[0];

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var t in tables)
                t.Sort();

            // Return the final query
            var query = new UpdateQuery
            {
                FetchXml = fetch,
                PostFilter = postFilter,
                EntityName = table.EntityName,
                IdColumn = cols[0],
                Updates = updates,
                AllPages = fetch.page == null && fetch.top == null
            };

            if (postFilter != null && fetch.top != null)
            {
                query.PostTop = Int32.Parse(fetch.top);
                fetch.top = null;
            }

            return query;
        }

        /// <summary>
        /// Converts attribute values to the appropriate type for INSERT and UPDATE queries
        /// </summary>
        /// <param name="metadata">The metadata of the entity being affected</param>
        /// <param name="values">A mapping of attribute name to value</param>
        /// <returns>A mapping of attribute name to value</returns>
        private IDictionary<string, object> ConvertAttributeValueTypes(EntityMetadata metadata, IDictionary<string, string> values)
        {
            return values
                .ToDictionary(kvp => kvp.Key, kvp => ConvertAttributeValueType(metadata, kvp.Key, kvp.Value));
        }

        /// <summary>
        /// Converts an attribute value to the appropriate type for INSERT and UPDATE queries
        /// </summary>
        /// <param name="metadata">The metadata of the entity being affected</param>
        /// <param name="attrName">The name of the attribute</param>
        /// <param name="value">The value for the attribute</param>
        /// <returns>The <paramref name="value"/> converted to the type appropriate for the <paramref name="attrName"/></returns>
        private object ConvertAttributeValueType(EntityMetadata metadata, string attrName, string value)
        {
            // Don't care about types for nulls
            if (value == null)
                return null;

            // Find the correct attribute
            var attr = metadata.Attributes.SingleOrDefault(a => a.LogicalName == attrName);

            if (attr == null)
                throw new NotSupportedException("Unknown attribute " + attrName);

            // Handle the conversion for each attribute type
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

        /// <summary>
        /// Converts the SET clause of an UPDATE statement to a mapping of attribute name to the value to set it to
        /// </summary>
        /// <param name="setClauses">The SET clause to convert</param>
        /// <param name="tables">The tables in the FROM clause of the query</param>
        /// <param name="metadata">The metadata of the entity to be updated</param>
        /// <returns>A mapping of attribute name to value extracted from the <paramref name="setClauses"/></returns>
        private IDictionary<string,Func<Entity,object>> HandleSetClause(IList<SetClause> setClauses, List<EntityTable> tables, EntityMetadata metadata)
        {
            return setClauses
                .Select(set =>
                {
                    // Check for unsupported SQL DOM elements
                    if (!(set is AssignmentSetClause assign))
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", set);

                    if (assign.Column == null)
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", assign);

                    if (assign.Column.MultiPartIdentifier.Identifiers.Count > 1)
                        throw new NotSupportedQueryFragmentException("Unsupported UPDATE SET clause", assign.Column);

                    var attrName = assign.Column.MultiPartIdentifier.Identifiers[0].Value.ToLower();
                    var attr = metadata.Attributes.SingleOrDefault(a => a.LogicalName == attrName);
                    if (attr == null)
                        throw new NotSupportedQueryFragmentException("Unknown column name", assign.Column);

                    if (assign.NewValue is Literal literal)
                    {
                        // Handle updates to literal values
                        // Special case for null, otherwise the value is extracted as a string, so convert it to the required type
                        if (literal is NullLiteral)
                        {
                            return new { Key = attrName, Value = (Func<Entity,object>) (e => null) };
                        }
                        else
                        {
                            var value = ConvertAttributeValueType(metadata, attrName, literal.Value);
                            return new { Key = attrName, Value = (Func<Entity, object>)(e => value) };
                        }
                    }
                    else
                    {
                        // Handle updates to the value from another field
                        // Ensure the source field is included in the query
                        return new { Key = attrName, Value = CompileScalarExpression<object>(assign.NewValue, tables, false, null, out _) };
                    }
                })
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        private Func<Entity,T> CompileScalarExpression<T>(ScalarExpression expr, List<EntityTable> tables, bool aggregate, IDictionary<string,Expression> calculatedFields, out Expression expression)
        {
            expression = ConvertScalarExpression(expr, tables, aggregate, calculatedFields, _param);

            expression = Expr.Convert<T>(expression);

            return Expression.Lambda<Func<Entity, T>>(expression, _param).Compile();
        }

        /// <summary>
        /// Converts a scalar SQL expression to a compiled function that evaluates the final value of the expression for a given entity.
        /// </summary>
        /// <param name="expr">The <see cref="ScalarExpression"/> to convert</param>
        /// <param name="tables">The tables to use as the data source for the expression</param>
        /// <param name="aggregate">Indicates if the query is an aggregate query</param>
        /// <param name="calculatedFields">Any calculated fields that will be created when running the query</param>
        /// <returns>A compiled expression that evaluates the supplied <paramref name="expr"/> for a given entity</returns>
        /// <remarks>
        /// This method will ensure any attributes required by the <paramref name="expr"/> are added to the query.
        /// </remarks>
        private Expression ConvertScalarExpression(ScalarExpression expr, List<EntityTable> tables, bool aggregate, IDictionary<string,Expression> calculatedFields, ParameterExpression param)
        {
            if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression binary)
            {
                // Binary operator - convert the two operands to expressions first
                var lhs = ConvertScalarExpression(binary.FirstExpression, tables, aggregate, calculatedFields, param);
                var rhs = ConvertScalarExpression(binary.SecondExpression, tables, aggregate, calculatedFields, param);

                // If either operand is a Nullable<T>, get the inner Value to use for the main operator
                var lhsValue = lhs;
                var rhsValue = rhs;

                if (lhsValue.Type.IsGenericType && lhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    lhsValue = Expression.Property(lhsValue, lhsValue.Type.GetProperty("Value"));

                if (rhsValue.Type.IsGenericType && rhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    rhsValue = Expression.Property(rhsValue, rhsValue.Type.GetProperty("Value"));

                Expression coreOperator;

                switch (binary.BinaryExpressionType)
                {
                    case BinaryExpressionType.Add:
                        if (lhs.Type == typeof(string))
                            coreOperator = Expression.Add(lhs, rhs, typeof(string).GetMethod("Concat", new[] { typeof(object), typeof(object) }));
                        else
                            coreOperator = Expression.Add(lhs, rhs);
                        break;

                    case BinaryExpressionType.BitwiseAnd:
                        coreOperator = Expression.And(lhs, rhs);
                        break;

                    case BinaryExpressionType.BitwiseOr:
                        coreOperator = Expression.Or(lhs, rhs);
                        break;

                    case BinaryExpressionType.BitwiseXor:
                        coreOperator = Expression.ExclusiveOr(lhs, rhs);
                        break;

                    case BinaryExpressionType.Divide:
                        coreOperator = Expression.Divide(lhs, rhs);
                        break;

                    case BinaryExpressionType.Modulo:
                        coreOperator = Expression.Modulo(lhs, rhs);
                        break;

                    case BinaryExpressionType.Multiply:
                        coreOperator = Expression.Multiply(lhs, rhs);
                        break;

                    case BinaryExpressionType.Subtract:
                        coreOperator = Expression.Subtract(lhs, rhs);
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unsupported operator", expr);
                }

                // Add null checking for all operator types
                Expression nullCheck = null;

                if (lhs.Type.IsClass || lhs.Type.IsGenericType && lhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    nullCheck = Expression.Equal(lhs, Expression.Constant(null));

                if (rhs.Type.IsClass || rhs.Type.IsGenericType && lhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var rhsNullCheck = Expression.Equal(rhs, Expression.Constant(null));

                    if (nullCheck == null)
                        nullCheck = rhsNullCheck;
                    else
                        nullCheck = Expression.OrElse(nullCheck, rhsNullCheck);
                }

                // If neither type was nullable, nothing to do
                if (nullCheck == null)
                    return coreOperator;

                // If the core operator returns non-nullable value (e.g. int + int), make it nullable (i.e. (int?) int + int)
                // so we can return a null value of the same type if either argument was actually null.
                var targetType = coreOperator.Type;

                if (targetType.IsPrimitive)
                {
                    targetType = typeof(Nullable<>).MakeGenericType(targetType);
                    coreOperator = Expression.Convert(coreOperator, targetType);
                }

                // Final result for int? lhs + int? rhs is
                // lhs == null || rhs == null ? (int?) null : (int?) (lhs.Value + rhs.Value)
                return Expression.Condition(
                    test: nullCheck,
                    ifTrue: Expression.Convert(Expression.Constant(null), targetType),
                    ifFalse: coreOperator);
            }
            else if (expr is ColumnReferenceExpression col)
            {
                // Check if this attribute is really a calculated field
                if (col.MultiPartIdentifier.Identifiers.Count == 1 && calculatedFields != null && calculatedFields.TryGetValue(col.MultiPartIdentifier.Identifiers[0].Value, out var calculated))
                    return calculated;

                // Check where this attribute should be taken from
                var tableAlias = GetColumnTableAlias(col, tables, out var table);
                var attrName = GetColumnAttribute(col);
                
                // Check if the attribute needs to be added to the query
                if (!table.Contains(i => i is allattributes) &&
                    !table.Contains(i => i is FetchAttributeType attr && attr.name == attrName && attr.alias == null && !attr.aggregateSpecified && !attr.dategroupingSpecified))
                {
                    // Can't add the column in aggregate queries without breaking the grouping
                    if (aggregate)
                        throw new NotSupportedQueryFragmentException("Column is not included in a GROUP BY expression", expr);

                    table.AddItem(new FetchAttributeType { name = attrName });
                }

                var attrMetadata = Metadata[table.EntityName].Attributes.SingleOrDefault(a => a.LogicalName == attrName);
                if (attrMetadata == null)
                    throw new NotSupportedQueryFragmentException("Unknown attribute", expr);

                if (attrMetadata.AttributeType == null)
                    throw new NotSupportedQueryFragmentException("Unknown attribute type", expr);

                var type = GetAttributeType(attrMetadata.AttributeType.Value);

                if (type == null)
                    throw new NotSupportedQueryFragmentException("Unknown attribute type", expr);

                if (tableAlias != null)
                    attrName = tableAlias + "." + attrName;
                
                return
                    Expression.Convert(
                        Expression.Call(
                            null, 
                            typeof(Sql2FetchXml).GetMethod(nameof(GetAttributeValue), BindingFlags.Static | BindingFlags.NonPublic), 
                            param, 
                            Expression.Constant(attrName)
                        ),
                        type);
            }
            else if (expr is Literal literal)
            {
                return Expression.Constant(ConvertLiteralValue(literal));
            }
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary)
            {
                var child = ConvertScalarExpression(unary.Expression, tables, aggregate, calculatedFields, param);

                switch (unary.UnaryExpressionType)
                {
                    case UnaryExpressionType.BitwiseNot:
                        return Expression.Not(child);

                    case UnaryExpressionType.Negative:
                        return Expression.Negate(child);

                    case UnaryExpressionType.Positive:
                        return child;
                }
            }
            else if (expr is FunctionCall func)
            {
                // Function calls become method calls. All allowed methods are statics in the ExpressionFunctions class
                var method = typeof(ExpressionFunctions).GetMethod(func.FunctionName.Value, BindingFlags.Static | BindingFlags.Public | BindingFlags.IgnoreCase);

                if (method == null)
                    throw new NotSupportedQueryFragmentException("Unknown function", func);

                var parameters = method.GetParameters();

                if (func.Parameters.Count != parameters.Length)
                    throw new NotSupportedQueryFragmentException($"{method.Name} function requires {parameters.Length} parameter(s)", func);

                Expression[] args;

                switch (func.FunctionName.Value.ToLower())
                {
                    case "dateadd":
                    case "datediff":
                    case "datepart":
                        // Special case for datepart argument
                        if (!(func.Parameters[0] is ColumnReferenceExpression datepart))
                            throw new NotSupportedQueryFragmentException("Invalid DATEPART argument", func.Parameters[0]);

                        args = new Expression[parameters.Length];
                        args[0] = Expression.Constant(datepart.MultiPartIdentifier.Identifiers[0].Value);

                        for (var i = 1; i < args.Length; i++)
                            args[i] = ConvertScalarExpression(func.Parameters[i], tables, aggregate, calculatedFields, param);
                        break;

                    default:
                        args = func.Parameters
                            .Select(p => ConvertScalarExpression(p, tables, aggregate, calculatedFields, param))
                            .ToArray();
                        break;
                }

                return Expr.Call(method, args);
            }
            else if (expr is SearchedCaseExpression searchedCase)
            {
                // CASE WHEN field = 1 THEN 'Big' WHEN field = 2 THEN 'Small' ELSE 'Unknown' END
                // becomes
                // field = 1 ? 'Big' : field = 2 ? 'Small' : 'Unknown'
                // Build the expression up from the end and work backwards
                var converted = searchedCase.ElseExpression == null ? Expression.Constant(null) : ConvertScalarExpression(searchedCase.ElseExpression, tables, aggregate, calculatedFields, param);

                foreach (var when in searchedCase.WhenClauses.Reverse())
                {
                    converted = Expression.Condition(
                        test: HandleFilterPredicate(when.WhenExpression, tables, aggregate, param),
                        ifTrue: ConvertScalarExpression(when.ThenExpression, tables, aggregate, calculatedFields, param),
                        ifFalse: converted);
                }

                return converted;
            }
            else if (expr is SimpleCaseExpression simpleCase)
            {
                // CASE field WHEN 1 THEN 'Big' WHEN 2 THEN 'Small' ELSE 'Unknown' END
                // becomes
                // field = 1 ? 'Big' : field = 2 ? 'Small' : 'Unknown'
                // Build the expression up from the end and work backwards
                var value = ConvertScalarExpression(simpleCase.InputExpression, tables, aggregate, calculatedFields, param);
                var converted = simpleCase.ElseExpression == null ? Expression.Constant(null) : ConvertScalarExpression(simpleCase.ElseExpression, tables, aggregate, calculatedFields, param);

                foreach (var when in simpleCase.WhenClauses.Reverse())
                {
                    converted = Expression.Condition(
                        test: Expression.Equal(value, ConvertScalarExpression(when.WhenExpression, tables, aggregate, calculatedFields, param)),
                        ifTrue: ConvertScalarExpression(when.ThenExpression, tables, aggregate, calculatedFields, param),
                        ifFalse: converted);
                }

                return converted;
            }

            throw new NotSupportedQueryFragmentException("Unsupported expression", expr);
        }

        private static Type GetAttributeType(AttributeTypeCode type)
        {
            switch (type)
            {
                case AttributeTypeCode.BigInt:
                    return typeof(long?);

                case AttributeTypeCode.Boolean:
                    return typeof(bool?);

                case AttributeTypeCode.Customer:
                    return typeof(EntityReference);

                case AttributeTypeCode.DateTime:
                    return typeof(DateTime?);

                case AttributeTypeCode.Decimal:
                    return typeof(decimal?);

                case AttributeTypeCode.Double:
                    return typeof(double?);

                case AttributeTypeCode.EntityName:
                    return typeof(string);

                case AttributeTypeCode.Integer:
                    return typeof(int?);

                case AttributeTypeCode.Lookup:
                    return typeof(EntityReference);

                case AttributeTypeCode.Memo:
                    return typeof(string);

                case AttributeTypeCode.Money:
                    return typeof(decimal?);

                case AttributeTypeCode.Owner:
                    return typeof(EntityReference);

                case AttributeTypeCode.Picklist:
                case AttributeTypeCode.State:
                case AttributeTypeCode.Status:
                    return typeof(int?);

                case AttributeTypeCode.String:
                    return typeof(string);

                case AttributeTypeCode.Uniqueidentifier:
                    return typeof(Guid?);
            }

            return null;
        }

        private static object GetAttributeValue(Entity entity, string attrName)
        {
            if (!entity.Attributes.TryGetValue(attrName, out var value))
                return null;

            if (value is AliasedValue alias)
                value = alias.Value;

            if (value is OptionSetValue osv)
                return osv.Value;

            if (value is Money money)
                return money.Value;

            return value;
        }

        private object ConvertLiteralValue(Literal literal)
        {
            switch (literal.LiteralType)
            {
                case LiteralType.Integer:
                    return Int32.Parse(literal.Value);

                case LiteralType.Money:
                case LiteralType.Numeric:
                    return Decimal.Parse(literal.Value);

                case LiteralType.Null:
                    return null;

                case LiteralType.Real:
                    return Double.Parse(literal.Value);

                case LiteralType.String:
                    return literal.Value;
            }

            throw new NotSupportedQueryFragmentException("Unsupported expression", literal);
        }

        /// <summary>
        /// Converts an UPDATE query
        /// </summary>
        /// <param name="update">The SQL query to convert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private SelectQuery ConvertSelectStatement(SelectStatement select)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
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

            // Convert the FROM clause first so we've got the context for each other clause
            var fetch = new FetchXml.FetchType();
            var tables = HandleFromClause(querySpec.FromClause, fetch);
            HandleSelectClause(querySpec, fetch, tables, out var columns, out var calculatedFields, out var calculatedFieldExpressions);
            HandleTopClause(querySpec.TopRowFilter, fetch);
            HandleOffsetClause(querySpec, fetch);
            HandleGroupByClause(querySpec, fetch, tables, columns);
            HandleWhereClause(querySpec.WhereClause, tables, fetch.aggregate, out var postFilter);
            var sorts = HandleOrderByClause(querySpec, fetch, tables, columns, calculatedFieldExpressions);
            HandleDistinctClause(querySpec, fetch);

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var table in tables)
                table.Sort();
            
            // Return the final query
            var query = new SelectQuery
            {
                FetchXml = fetch,
                PostFilter = postFilter,
                CalculatedFields = calculatedFields,
                PostSorts = sorts,
                ColumnSet = columns,
                AllPages = fetch.page == null && fetch.count == null
            };

            if ((postFilter != null || sorts != null) && fetch.top != null)
            {
                query.PostTop = Int32.Parse(fetch.top);
                fetch.top = null;
            }

            return query;
        }

        /// <summary>
        /// Converts the GROUP BY clause of a SELECT statement to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the GROUP BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        private void HandleGroupByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
            if (querySpec.GroupByClause == null)
                return;

            if (querySpec.GroupByClause.All == true)
                throw new NotSupportedQueryFragmentException("Unhandled GROUP BY ALL clause", querySpec.GroupByClause);

            if (querySpec.GroupByClause.GroupByOption != GroupByOption.None)
                throw new NotSupportedQueryFragmentException("Unhandled GROUP BY option", querySpec.GroupByClause);

            // Set the aggregate flag on the FetchXML query
            fetch.aggregate = true;
            fetch.aggregateSpecified = true;

            // Process each field that the data should be grouped by
            foreach (var group in querySpec.GroupByClause.GroupingSpecifications)
            {
                // Check this is a grouping type we understand. This can be a simple attribute or a date part
                if (!(group is ExpressionGroupingSpecification exprGroup))
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY clause", group);

                var expr = exprGroup.Expression;
                DateGroupingType? dateGrouping = null;

                if (expr is FunctionCall func)
                {
                    if (!TryParseDatePart(func, out var g, out var dateCol))
                        throw new NotSupportedQueryFragmentException("Unhandled GROUP BY clause", expr);

                    dateGrouping = g;
                    expr = dateCol;
                }

                if (!(expr is ColumnReferenceExpression col))
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY clause", exprGroup.Expression);

                // Find the table in the query that the grouping attribute is from
                GetColumnTableAlias(col, tables, out var table);

                if (table == null)
                    throw new NotSupportedQueryFragmentException("Unknown table", col);

                // Check if the attribute has already been added to the FetchXML query. It usually will because you'd normally
                // include the same attribute in the SELECT clause, but it's possible to group the data by an attribute that isn't
                // included in the result
                var attr = (table.Entity?.Items ?? table.LinkEntity.Items)
                    .OfType<FetchAttributeType>()
                    .Where(a => a.name == col.MultiPartIdentifier.Identifiers.Last().Value && (a.dategroupingSpecified == false && dateGrouping == null || (a.dategroupingSpecified && dateGrouping != null && a.dategrouping == dateGrouping.Value)))
                    .SingleOrDefault();

                if (attr == null)
                {
                    // If the attribute isn't already included, add it to the appropriate table
                    attr = new FetchAttributeType { name = col.MultiPartIdentifier.Identifiers.Last().Value };

                    if (dateGrouping != null)
                    {
                        attr.dategrouping = dateGrouping.Value;
                        attr.dategroupingSpecified = true;
                    }

                    table.AddItem(attr);
                }

                // Groupings in FetchXML must have aliases, so create an alias if we don't already have one
                if (attr.alias == null)
                {
                    attr.alias = attr.name;

                    // Once we've given the attribute an alias, we might need to change the output column list to use the alias instead of
                    // the table-qualified name
                    for (var i = 0; i < columns.Length; i++)
                    {
                        if (columns[i].Equals($"{table.Alias ?? table.EntityName}.{attr.name}", StringComparison.OrdinalIgnoreCase))
                            columns[i] = attr.name;
                    }
                }

                // Ensure the grouping flag is set on the attribute
                attr.groupby = FetchBoolType.@true;
                attr.groupbySpecified = true;
            }
        }

        /// <summary>
        /// Convert a DATEPART(part, attribute) function call to the appropriate <see cref="DateGroupingType"/> and attribute name
        /// </summary>
        /// <param name="func">The function call to attempt to parse as a DATEPART function</param>
        /// <param name="dateGrouping">The <see cref="DateGroupingType"/> extracted from the <paramref name="func"/></param>
        /// <param name="col">The attribute details extracted from the <paramref name="func"/></param>
        /// <returns><c>true</c> if the <paramref name="func"/> is successfully identified as a supported DATEPART function call, or <c>false</c> otherwise</returns>
        private bool TryParseDatePart(FunctionCall func, out DateGroupingType dateGrouping, out ColumnReferenceExpression col)
        {
            dateGrouping = DateGroupingType.day;
            col = null;

            // Check that the function has the expected name and number of parameters
            if (!func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) || func.Parameters.Count != 2 || !(func.Parameters[0] is ColumnReferenceExpression datePartParam))
                return false;

            // Check that the second parameter is a reference to a column
            col = func.Parameters[1] as ColumnReferenceExpression;

            if (col == null)
                return false;

            // Convert the first parameter to the correct DateGroupingType
            switch (datePartParam.MultiPartIdentifier[0].Value.ToLower())
            {
                case "year":
                case "yy":
                case "yyyy":
                    dateGrouping = DateGroupingType.year;
                    break;

                case "quarter":
                case "qq":
                case "q":
                    dateGrouping = DateGroupingType.quarter;
                    break;

                case "month":
                case "mm":
                case "m":
                    dateGrouping = DateGroupingType.month;
                    break;

                case "week":
                case "wk":
                case "ww":
                    dateGrouping = DateGroupingType.week;
                    break;

                case "day":
                case "dd":
                case "d":
                    dateGrouping = DateGroupingType.day;
                    break;

                // These last two are not in the T-SQL spec, but are CDS-specific extensions
                case "fiscalperiod":
                    dateGrouping = DateGroupingType.fiscalperiod;
                    break;

                case "fiscalyear":
                    dateGrouping = DateGroupingType.fiscalyear;
                    break;

                default:
                    throw new NotSupportedQueryFragmentException("Unsupported DATEPART", datePartParam);
            }

            return true;
        }

        /// <summary>
        /// Converts the DISTINCT clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the GROUP BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        private void HandleDistinctClause(QuerySpecification querySpec, FetchXml.FetchType fetch)
        {
            if (querySpec.UniqueRowFilter == UniqueRowFilter.Distinct)
            {
                fetch.distinct = true;
                fetch.distinctSpecified = true;
            }
        }

        /// <summary>
        /// Converts the OFFSET clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the OFFSET clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        private void HandleOffsetClause(QuerySpecification querySpec, FetchXml.FetchType fetch)
        {
            // The OFFSET clause doesn't have a direct equivalent in FetchXML, but in some circumstances we can get the same effect
            // by going direct to a specific page. For this to work the offset must be an exact multiple of the fetch count
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

        /// <summary>
        /// Converts the TOP clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="top">The TOP clause of the SELECT query to convert from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        private void HandleTopClause(TopRowFilter top, FetchXml.FetchType fetch)
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

        /// <summary>
        /// Converts the ORDER BY clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the ORDER BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        /// <returns>The sorts to apply to the results of the query</returns>
        private SortExpression[] HandleOrderByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Expression> calculatedFieldExpressions)
        {
            if (querySpec.OrderByClause == null)
                return null;

            try
            {
                HandleOrderByClauseFetchXml(querySpec, fetch, tables, columns, calculatedFieldExpressions);
                return null;
            }
            catch (PostProcessingRequiredException)
            {
                return HandleOrderByClauseExpression(querySpec, fetch, tables, columns, calculatedFieldExpressions);
            }
        }

        /// <summary>
        /// Converts the ORDER BY clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the ORDER BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        private void HandleOrderByClauseFetchXml(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Expression> calculatedFieldExpressions)
        {
            // Convert each ORDER BY expression in turn
            foreach (var sort in querySpec.OrderByClause.OrderByElements)
            {
                // Each sort should be either a column or a number representing the index (1 based) of the column in the output dataset
                // to order by. For aggregate queries it could also be an aggregate function
                if (!(sort.Expression is ColumnReferenceExpression col))
                {
                    if (sort.Expression is IntegerLiteral colIndex)
                    {
                        var colName = columns[Int32.Parse(colIndex.Value) - 1];

                        if (calculatedFieldExpressions != null && calculatedFieldExpressions.ContainsKey(colName))
                            throw new PostProcessingRequiredException("Cannot sort by calculated field", sort.Expression);

                        col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

                        foreach (var part in colName.Split('.'))
                            col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });
                    }
                    else if (fetch.aggregateSpecified && fetch.aggregate && sort.Expression is FunctionCall)
                    {
                        // Check if we already have this aggregate as an attribute. If not, add it.
                        var alias = "";
                        AddAttribute(fetch, tables, new List<string>(columns), sort.Expression, out var table, out var attr, ref alias);

                        // Finally, sort by the alias of the aggregate attribute
                        col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };
                        col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = alias });
                    }
                    else
                    {
                        throw new PostProcessingRequiredException("Unsupported ORDER BY clause", sort.Expression);
                    }
                }

                if (col.MultiPartIdentifier.Identifiers.Count == 1 && calculatedFieldExpressions != null && calculatedFieldExpressions.ContainsKey(col.MultiPartIdentifier.Identifiers[0].Value))
                    throw new PostProcessingRequiredException("Cannot sort by calculated field", sort.Expression);

                // Find the table from which the column is taken
                GetColumnTableAlias(col, tables, out var orderTable);

                // Can't control sequence of orders between link-entities. Orders are always applied in depth-first-search order, so
                // check there is no order already applied on a later entity.
                if (LaterEntityHasOrder(tables, orderTable))
                    throw new PostProcessingRequiredException("Order already applied to later link-entity", sort.Expression);
                
                var order = new FetchOrderType
                {
                    attribute = GetColumnAttribute(col),
                    descending = sort.SortOrder == SortOrder.Descending
                };

                // For aggregate queries, ordering must be done on aliases not attributes
                if (fetch.aggregate)
                {
                    var attr = (orderTable.Entity?.Items ?? orderTable.LinkEntity?.Items)
                        .OfType<FetchAttributeType>()
                        .SingleOrDefault(a => a.alias == order.attribute);

                    if (attr == null)
                    {
                        attr = (orderTable.Entity?.Items ?? orderTable.LinkEntity?.Items)
                            .OfType<FetchAttributeType>()
                            .SingleOrDefault(a => a.alias == null && a.name == order.attribute);
                    }

                    if (attr == null)
                        throw new NotSupportedQueryFragmentException("Column is invalid in the ORDER BY clause because it is not contained in either an aggregate function or the GROUP BY clause", sort.Expression);

                    if (attr.alias == null)
                        attr.alias = order.attribute;

                    order.alias = attr.alias;
                    order.attribute = null;
                }

                // Paging has a bug if the orderby attribute is included but has a different alias. In this case,
                // add the attribute again without an alias
                if (!fetch.aggregateSpecified || !fetch.aggregate)
                {
                    var containsAliasedAttr = orderTable.Contains(i => i is FetchAttributeType a && a.name.Equals(order.attribute) && a.alias != null && a.alias != a.name);

                    if (containsAliasedAttr)
                        orderTable.AddItem(new FetchAttributeType { name = order.attribute });
                }

                orderTable.AddItem(order);
            }
        }


        /// <summary>
        /// Converts the ORDER BY clause of a SELECT query to expressions
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the ORDER BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        private SortExpression[] HandleOrderByClauseExpression(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Expression> calculatedFieldExpressions)
        {
            // Check how many sorts were already converted to native FetchXML - we can use these results to only sort partial sequences
            // of results rather than having to sort the entire result set in memory.
            var fetchXmlSorts = tables.SelectMany(t => (t.Entity?.Items ?? t.LinkEntity?.Items).OfType<FetchOrderType>()).Count();

            // Convert each ORDER BY expression in turn
            var sortNumber = 0;
            var sorts = new List<SortExpression>();

            foreach (var sort in querySpec.OrderByClause.OrderByElements)
            {
                var expression = sort.Expression;

                if (expression is IntegerLiteral colIndex)
                {
                    var colName = columns[Int32.Parse(colIndex.Value) - 1];

                    expression = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

                    foreach (var part in colName.Split('.'))
                        ((ColumnReferenceExpression)expression).MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });
                }

                sortNumber++;
                var isFetchXml = sortNumber <= fetchXmlSorts;
                var selector = CompileScalarExpression<IComparable>(expression, tables, fetch.aggregateSpecified && fetch.aggregate, calculatedFieldExpressions, out _);
                var descending = sort.SortOrder == SortOrder.Descending;

                sorts.Add(new SortExpression(isFetchXml, selector, descending));
            }

            return sorts.ToArray();
        }

        /// <summary>
        /// Check if a later table (in DFS order) than the <paramref name="orderTable"/> has got a sort applied
        /// </summary>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="orderTable">The table to check from</param>
        /// <returns></returns>
        private bool LaterEntityHasOrder(List<EntityTable> tables, EntityTable orderTable)
        {
            var passedOrderTable = false;
            return LaterEntityHasOrder(tables, tables[0], orderTable, ref passedOrderTable);
        }

        /// <summary>
        /// Check if a later table (in DFS order) than the <paramref name="orderTable"/> has got a sort applied
        /// </summary>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="entityTable">The current table being considered</param>
        /// <param name="orderTable">The table to check from</param>
        /// <param name="passedOrderTable">Indicates if the DFS has passed the <paramref name="orderTable"/> yet</param>
        /// <returns></returns>
        private bool LaterEntityHasOrder(List<EntityTable> tables, EntityTable entityTable, EntityTable orderTable, ref bool passedOrderTable)
        {
            var items = (entityTable.Entity?.Items ?? entityTable.LinkEntity?.Items);

            if (items == null)
                return false;

            if (passedOrderTable && items.OfType<FetchOrderType>().Any())
                return true;

            if (entityTable == orderTable)
                passedOrderTable = true;

            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                var linkTable = tables.Single(t => t.LinkEntity == link);
                if (LaterEntityHasOrder(tables, linkTable, orderTable, ref passedOrderTable))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Converts the WHERE clause of a query to FetchXML
        /// </summary>
        /// <param name="where">The WHERE clause to convert</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="aggregate">Indicates if the query is an aggregate query</param>
        /// <param name="postFilter">The filter to be applied in memory</param>
        private void HandleWhereClause(WhereClause where, List<EntityTable> tables, bool aggregate, out Func<Entity,bool> postFilter)
        {
            postFilter = null;

            // Check for any DOM elements that don't have an equivalent in CDS
            if (where == null)
                return;

            if (where.Cursor != null)
                throw new NotSupportedQueryFragmentException("Unhandled WHERE clause", where.Cursor);

            // Start with a filter with an indeterminate logical operator
            var filter = new filter
            {
                type = (filterType)2
            };

            tables[0].AddItem(filter);

            // Add the conditions into the filter
            ColumnReferenceExpression col1 = null;
            ColumnReferenceExpression col2 = null;
            Expression postFilterExpression = null;
            var param = Expression.Parameter(typeof(Entity), "entity");
            HandleFilter(where.SearchCondition, filter, tables, tables[0], true, ref col1, ref col2, aggregate, ref postFilterExpression, param);

            // If no specific logical operator was found, switch to "and"
            if (filter.type == (filterType)2)
                filter.type = filterType.and;

            if (postFilterExpression != null)
                postFilter = Expression.Lambda<Func<Entity, bool>>(postFilterExpression, param).Compile();
        }

        /// <summary>
        /// Converts filter criteria to FetchXML
        /// </summary>
        /// <param name="searchCondition">The SQL filter to convert from</param>
        /// <param name="criteria">The FetchXML to convert to</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="targetTable">The table that the filters will be applied to</param>
        /// <param name="where">Indicates if the filters are part of a WHERE clause</param>
        /// <param name="inOr">Indicates if the filter is within an OR filter</param>
        /// <param name="col1">Identifies the first column in the filter (for JOIN purposes)</param>
        /// <param name="col2">Identifies the second column in the filter (for JOIN purposes)</param>
        /// <param name="aggregate">Indicates if the query is an aggregate query</param>
        /// <param name="postFilter">The additional in-memory predicate to be applied to any results</param>
        /// <param name="param">The entity parameter to be supplied to the <paramref name="postFilter"/></param>
        private void HandleFilter(BooleanExpression searchCondition, filter criteria, List<EntityTable> tables, EntityTable targetTable, bool where, ref ColumnReferenceExpression col1, ref ColumnReferenceExpression col2, bool aggregate, ref Expression postFilter, ParameterExpression param)
        {
            try
            {
                HandleFilterFetchXml(searchCondition, criteria, tables, targetTable, where, false, ref col1, ref col2, aggregate, ref postFilter, param);
            }
            catch (PostProcessingRequiredException)
            {
                if (!where)
                    throw;

                var predicate = HandleFilterPredicate(searchCondition, tables, aggregate, param);

                if (postFilter == null)
                    postFilter = predicate;
                else
                    postFilter = Expression.And(postFilter, predicate);
            }
        }

        /// <summary>
        /// Converts filter criteria to FetchXML
        /// </summary>
        /// <param name="searchCondition">The SQL filter to convert from</param>
        /// <param name="criteria">The FetchXML to convert to</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="targetTable">The table that the filters will be applied to</param>
        /// <param name="where">Indicates if the filters are part of a WHERE clause</param>
        /// <param name="inOr">Indicates if the filter is within an OR filter</param>
        /// <param name="col1">Identifies the first column in the filter (for JOIN purposes)</param>
        /// <param name="col2">Identifies the second column in the filter (for JOIN purposes)</param>
        /// <param name="aggregate">Indicates if the query is an aggregate query</param>
        /// <param name="postFilter">The additional in-memory predicate to be applied to any results</param>
        /// <param name="param">The entity parameter to be supplied to the <paramref name="postFilter"/></param>
        private void HandleFilterFetchXml(BooleanExpression searchCondition, filter criteria, List<EntityTable> tables, EntityTable targetTable, bool where, bool inOr, ref ColumnReferenceExpression col1, ref ColumnReferenceExpression col2, bool aggregate, ref Expression postFilter, ParameterExpression param)
        {
            if (searchCondition is BooleanComparisonExpression comparison)
            {
                // Handle most comparison operators (=, <> etc.)
                // Comparison can be between a column and either a literal value, function call or another column (for joins only)
                // Function calls are used to represent more complex FetchXML query operators
                // Operands could be in either order, so `column = 'value'` or `'value' = column` should both be allowed
                var field = comparison.FirstExpression as ColumnReferenceExpression;
                var literal = comparison.SecondExpression as Literal;
                var func = comparison.SecondExpression as FunctionCall;
                var field2 = comparison.SecondExpression as ColumnReferenceExpression;
                var type = comparison.ComparisonType;

                if (field != null && field2 != null)
                {
                    // The operator is comparing two attributes. This is not allowed in a FetchXML filter,
                    // but is allowed in join criteria
                    if (where)
                        throw new PostProcessingRequiredException("Unsupported comparison", comparison);

                    if (col1 == null && col2 == null)
                    {
                        // We've found the join columns - don't apply this as an extra filter
                        if (inOr)
                            throw new NotSupportedQueryFragmentException("Cannot combine join criteria with OR", comparison);

                        col1 = field;
                        col2 = field2;
                        return;
                    }

                    throw new NotSupportedQueryFragmentException("Unsupported comparison", comparison);
                }

                // If we couldn't find the pattern `column = value` or `column = func()`, try looking in the opposite order
                if (field == null && literal == null && func == null)
                {
                    field = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression as Literal;
                    func = comparison.FirstExpression as FunctionCall;

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
                if (field == null || (literal == null && func == null))
                    throw new PostProcessingRequiredException("Unsupported comparison", comparison);

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
                
                object value = null;

                if (literal != null)
                {
                    // Convert the literal value to the correct type, if specified
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
                    // If we've got the pattern `column = func()`, select the FetchXML operator from the function name
                    op = (@operator) Enum.Parse(typeof(@operator), func.FunctionName.Value.ToLower());

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

                    if (func.Parameters.Count > 1)
                        throw new NotSupportedQueryFragmentException("Unsupported number of function parameters", func);

                    // Some advanced FetchXML operators use a value as well - take this as the function parameter
                    // This provides support for queries such as `createdon = lastxdays(3)` becoming <condition attribute="createdon" operator="last-x-days" value="3" />
                    if (func.Parameters.Count == 1)
                    {
                        if (!(func.Parameters[0] is Literal paramLiteral))
                            throw new NotSupportedQueryFragmentException("Unsupported function parameter", func.Parameters[0]);

                        value = paramLiteral.Value;
                    }
                    else if (func.Parameters.Count > 1)
                    {
                        // Only functions with 0 or 1 parameters are supported in FetchXML
                        throw new NotSupportedQueryFragmentException("Too many function parameters", func);
                    }
                }
                else
                {
                    // Can't use functions with other operators
                    throw new NotSupportedQueryFragmentException("Unsupported function use. Only <field> = <func>(<param>) usage is supported", comparison);
                }

                // Find the entity that the condition applies to, which may be different to the entity that the condition FetchXML element will be 
                // added within
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
                // Handle AND and OR conditions. If we're within the original <filter> and we haven't determined the type of that filter yet,
                // use that same filter. Otherwise, if we're switching to a different filter type, create a new sub-filter and add it in
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

                // Recurse into the sub-expressions
                try
                {
                    HandleFilterFetchXml(binary.FirstExpression, criteria, tables, targetTable, where, inOr || op == filterType.or, ref col1, ref col2, aggregate, ref postFilter, param);
                }
                catch (PostProcessingRequiredException)
                {
                    if (inOr || op != filterType.and || !where)
                        throw;

                    var lhsPredicate = HandleFilterPredicate(binary.FirstExpression, tables, aggregate, param);
                    if (postFilter == null)
                        postFilter = lhsPredicate;
                    else
                        postFilter = Expression.And(postFilter, lhsPredicate);
                }

                try
                {
                    HandleFilterFetchXml(binary.SecondExpression, criteria, tables, targetTable, where, inOr || op == filterType.or, ref col1, ref col2, aggregate, ref postFilter, param);
                }
                catch (PostProcessingRequiredException)
                {
                    if (inOr || op != filterType.and || !where)
                        throw;

                    var rhsPredicate = HandleFilterPredicate(binary.SecondExpression, tables, aggregate, param);
                    if (postFilter == null)
                        postFilter = rhsPredicate;
                    else
                        postFilter = Expression.And(postFilter, rhsPredicate);
                }
            }
            else if (searchCondition is BooleanParenthesisExpression paren)
            {
                // Create a new sub-filter to handle the contents of brackets, but we won't know the logical operator type to apply until
                // we encounter the first AND or OR within it
                var subFilter = new filter { type = (filterType)2 };
                criteria.Items = AddItem(criteria.Items, subFilter);
                criteria = subFilter;

                HandleFilterFetchXml(paren.Expression, criteria, tables, targetTable, where, inOr, ref col1, ref col2, aggregate, ref postFilter, param);

                if (subFilter.type == (filterType)2)
                    subFilter.type = filterType.and;
            }
            else if (searchCondition is BooleanIsNullExpression isNull)
            {
                // Handle IS NULL and IS NOT NULL expresisons
                if (!(isNull.Expression is ColumnReferenceExpression field))
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", isNull.Expression);

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = entityName,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = isNull.IsNot ? @operator.notnull : @operator.@null
                });
            }
            else if (searchCondition is LikePredicate like)
            {
                // Handle LIKE and NOT LIKE expressions. We can only support `column LIKE 'value'` expressions, not
                // `'value' LIKE column`
                if (!(like.FirstExpression is ColumnReferenceExpression field))
                    throw new PostProcessingRequiredException("Unsupported comparison", like.FirstExpression);

                if (!(like.SecondExpression is StringLiteral value))
                    throw new PostProcessingRequiredException("Unsupported comparison", like.SecondExpression);

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = entityName,
                    attribute = GetColumnAttribute(field),
                    @operator = like.NotDefined ? @operator.notlike : @operator.like,
                    value = value.Value
                });
            }
            else if (searchCondition is InPredicate @in)
            {
                // Handle IN and NOT IN expressions. We can only support `column IN ('value1', 'value2', ...)` expressions
                if (!(@in.Expression is ColumnReferenceExpression field))
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", @in.Expression);

                if (@in.Subquery != null)
                    throw new NotSupportedQueryFragmentException("Unsupported subquery, rewrite query as join", @in.Subquery);

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                var condition = new condition
                {
                    entityname = entityName,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = @in.NotDefined ? @operator.notin : @operator.@in
                };
                
                condition.Items = @in.Values
                    .Select(v =>
                    {
                        if (!(v is Literal literal))
                            throw new PostProcessingRequiredException("Unsupported comparison", v);

                        return new conditionValue
                        {
                            Value = literal.Value
                        };
                    })
                    .ToArray();

                criteria.Items = AddItem(criteria.Items, condition);
            }
            else
            {
                throw new PostProcessingRequiredException("Unhandled WHERE clause", searchCondition);
            }
        }


        /// <summary>
        /// Converts filter criteria to a predicate expression
        /// </summary>
        /// <param name="searchCondition">The SQL filter to convert from</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="aggregate">Indicates if the query is an aggregate query</param>
        /// <param name="param">The parameter that identifies the entity to apply the predicate to</param>
        /// <returns>The expression that implements the <paramref name="searchCondition"/></returns>
        private Expression HandleFilterPredicate(BooleanExpression searchCondition, List<EntityTable> tables, bool aggregate, ParameterExpression param)
        {
            if (searchCondition is BooleanComparisonExpression comparison)
            {
                var lhs = ConvertScalarExpression(comparison.FirstExpression, tables, aggregate, null, param);
                var rhs = ConvertScalarExpression(comparison.SecondExpression, tables, aggregate, null, param);

                // Type conversions for DateTime vs. string
                if (lhs.Type == typeof(DateTime?) && rhs.Type == typeof(string))
                    rhs = Expr.Convert<DateTime?>(rhs);
                else if (lhs.Type == typeof(string) && rhs.Type == typeof(DateTime?))
                    lhs = Expr.Convert<DateTime?>(lhs);

                var lhsValue = lhs;
                var rhsValue = rhs;

                if (lhsValue.Type.IsGenericType && lhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    lhsValue = Expression.Property(lhsValue, lhsValue.Type.GetProperty("Value"));

                if (rhsValue.Type.IsGenericType && rhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    rhsValue = Expression.Property(rhsValue, rhsValue.Type.GetProperty("Value"));

                Expression coreComparison;

                switch (comparison.ComparisonType)
                {
                    case BooleanComparisonType.Equals:
                        coreComparison = Expression.Equal(lhsValue, rhsValue);
                        break;

                    case BooleanComparisonType.GreaterThan:
                        coreComparison = Expression.GreaterThan(lhsValue, rhsValue);
                        break;

                    case BooleanComparisonType.GreaterThanOrEqualTo:
                        coreComparison = Expression.GreaterThanOrEqual(lhsValue, rhsValue);
                        break;

                    case BooleanComparisonType.LessThan:
                        coreComparison = Expression.LessThan(lhsValue, rhsValue);
                        break;

                    case BooleanComparisonType.LessThanOrEqualTo:
                        coreComparison = Expression.LessThanOrEqual(lhsValue, rhsValue);
                        break;

                    case BooleanComparisonType.NotEqualToBrackets:
                    case BooleanComparisonType.NotEqualToExclamation:
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue);
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unsupported comparison type", comparison);
                }

                // Add null checking for all comparison types
                Expression nullCheck = null;

                if (lhs.Type.IsClass || lhs.Type.IsGenericType && lhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    nullCheck = Expression.Equal(lhs, Expression.Constant(null));

                if (rhs.Type.IsClass || rhs.Type.IsGenericType && lhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var rhsNullCheck = Expression.Equal(rhs, Expression.Constant(null));

                    if (nullCheck == null)
                        nullCheck = rhsNullCheck;
                    else
                        nullCheck = Expression.OrElse(nullCheck, rhsNullCheck);
                }

                if (nullCheck == null)
                    return coreComparison;

                return Expression.Condition(
                    test: nullCheck,
                    ifTrue: Expression.Constant(false),
                    ifFalse: coreComparison);
            }
            else if (searchCondition is BooleanBinaryExpression binary)
            {
                var lhs = HandleFilterPredicate(binary.FirstExpression, tables, aggregate, param);
                var rhs = HandleFilterPredicate(binary.SecondExpression, tables, aggregate, param);

                if (binary.BinaryExpressionType == BooleanBinaryExpressionType.And)
                    return Expression.And(lhs, rhs);
                else
                    return Expression.Or(lhs, rhs);
            }
            else if (searchCondition is BooleanParenthesisExpression paren)
            {
                var child = HandleFilterPredicate(paren.Expression, tables, aggregate, param);
                return child;
            }
            else if (searchCondition is BooleanIsNullExpression isNull)
            {
                var value = ConvertScalarExpression(isNull.Expression, tables, aggregate, null, param);

                if (isNull.IsNot)
                    return Expression.NotEqual(value, Expression.Constant(null));
                else
                    return Expression.Equal(value, Expression.Constant(null));
            }
            else if (searchCondition is LikePredicate like)
            {
                var lhs = ConvertScalarExpression(like.FirstExpression, tables, aggregate, null, param);
                var rhs = ConvertScalarExpression(like.SecondExpression, tables, aggregate, null, param);
                var func = Expr.Call(() => ExpressionFunctions.Like(Expr.Arg<string>(), Expr.Arg<string>()),
                    lhs,
                    rhs);

                if (like.NotDefined)
                    return Expression.Not(func);
                else
                    return func;
            }
            else if (searchCondition is InPredicate @in)
            {
                if (@in.Subquery != null)
                    throw new NotSupportedQueryFragmentException("Unsupported subquery, rewrite query as join", @in.Subquery);

                Expression converted = null;
                var value = ConvertScalarExpression(@in.Expression, tables, aggregate, null, param);

                foreach (var v in @in.Values)
                {
                    var equal = Expression.Equal(value, ConvertScalarExpression(v, tables, aggregate, null, param));

                    if (converted == null)
                        converted = equal;
                    else
                        converted = Expression.Or(converted, equal);
                }

                return converted;
            }
            else
            {
                throw new PostProcessingRequiredException("Unhandled WHERE clause", searchCondition);
            }
        }

        /// <summary>
        /// Converts the SELECT clause of a query to FetchXML
        /// </summary>
        /// <param name="select">The SELECT query to convert</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        /// <param name="calculatedFields">The functions to use to generate calculated fields</param>
        /// <param name="calculatedFieldExpressions">The expressions that generate the calculated fields</param>
        private void HandleSelectClause(QuerySpecification select, FetchXml.FetchType fetch, List<EntityTable> tables, out string[] columns, out IDictionary<string,Func<Entity,object>> calculatedFields, out IDictionary<string,Expression> calculatedFieldExpressions)
        {
            var cols = new List<string>();
            calculatedFields = null;
            calculatedFieldExpressions = null;
            
            // Process each column in the SELECT list in turn
            foreach (var field in select.SelectElements)
            {
                if (field is SelectStarExpression star)
                {
                    // Handle SELECT * (i.e. all tables)
                    var starTables = tables;

                    // Handle SELECT table.*
                    if (star.Qualifier != null)
                        starTables = new List<EntityTable> { FindTable(star.Qualifier.Identifiers.Last().Value, tables, field) };

                    foreach (var starTable in starTables)
                    {
                        // If we're adding all attributes we can remove individual attributes
                        // FetchXML will ignore an <all-attributes> element if there are any individual <attribute> elements. We can cope
                        // with this by removing the <attribute> elements, but this won't give the expected results if any of the attributes
                        // have aliases
                        if (starTable.Contains(i => i is FetchAttributeType attr && attr.alias != null))
                            throw new NotSupportedQueryFragmentException("Cannot add aliased column and wildcard columns from same table", star);

                        starTable.RemoveItems(i => i is FetchAttributeType);

                        starTable.AddItem(new allattributes());

                        // We need to check the metadata to list all the columns we're going to include in the output dataset. Order these
                        // by name for a more readable result
                        var meta = Metadata[starTable.EntityName];

                        foreach (var attr in meta.Attributes.Where(a => a.IsValidForRead != false).OrderBy(a => a.LogicalName))
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
                    // Handle SELECT field, SELECT aggregate(field) and SELECT DATEPART(part, field)
                    var expr = scalar.Expression;

                    // Get the requested alias for the column, if any.
                    var alias = scalar.ColumnName?.Identifier?.Value;

                    try
                    {
                        AddAttribute(fetch, tables, cols, expr, out var table, out var attr, ref alias);

                        // Even if the attribute wasn't added to the entity because there's already an <all-attributes>, add it to the column list again
                        if (alias == null)
                            cols.Add((table.LinkEntity == null ? "" : ((table.Alias ?? table.EntityName) + ".")) + attr.name);
                        else
                            cols.Add(alias);
                    }
                    catch (NotSupportedQueryFragmentException)
                    {
                        // Try again converting to an expression
                        var func = CompileScalarExpression<object>(expr, tables, false, null, out var expression);

                        if (calculatedFields == null)
                            calculatedFields = new Dictionary<string, Func<Entity, object>>();

                        if (calculatedFieldExpressions == null)
                            calculatedFieldExpressions = new Dictionary<string, Expression>();

                        // Calculated field must have an alias, auto-generate one if one is not given
                        if (alias == null)
                            alias = $"Expr{cols.Count + 1}";

                        calculatedFields[alias] = func;
                        calculatedFieldExpressions[alias] = expression;
                        cols.Add(alias);
                    }
                }
                else
                {
                    // Any other expression type is not supported
                    throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", field);
                }
            }
            
            columns = cols.ToArray();
        }

        private void AddAttribute(FetchXml.FetchType fetch, List<EntityTable> tables, List<string> cols, ScalarExpression expr, out EntityTable table, out FetchAttributeType attr, ref string alias)
        {
            var originalExpr = expr;
            var func = expr as FunctionCall;

            if (func != null)
            {
                if (TryParseDatePart(func, out var dateGrouping, out var dateCol))
                {
                    // Rewrite DATEPART(part, field) as part(field) for simpler code later
                    func = new FunctionCall
                    {
                        FunctionName = new Identifier { Value = dateGrouping.ToString() },
                        Parameters = { dateCol }
                    };
                }

                // All function calls (aggregates) must be based on a single column
                if (func.Parameters.Count != 1)
                    throw new NotSupportedQueryFragmentException("Unhandled function", func);

                if (!(func.Parameters[0] is ColumnReferenceExpression colParam))
                    throw new NotSupportedQueryFragmentException("Unhandled function parameter", func.Parameters[0]);

                expr = colParam;
            }

            if (!(expr is ColumnReferenceExpression col))
                throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", originalExpr);

            // We now have a column, either on it's own or taken from the function parameter
            // Find the appropriate table and add the attribute to the table. For count(*), the "column"
            // is a wildcard type column - use the primary key column of the main entity instead
            string attrName;
            if (col.ColumnType == ColumnType.Wildcard)
                attrName = Metadata[tables[0].EntityName].PrimaryIdAttribute;
            else
                attrName = col.MultiPartIdentifier.Identifiers.Last().Value;

            if (col.ColumnType == ColumnType.Wildcard)
                table = tables[0];
            else
                GetColumnTableAlias(col, tables, out table);

            var matchAnyAlias = alias == "";
            attr = new FetchAttributeType { name = attrName };

            // If the column is being used within an aggregate or date grouping function, apply that now
            if (func != null)
            {
                switch (func.FunctionName.Value.ToLower())
                {
                    case "count":
                        // Select the appropriate aggregate depending on whether we're doing count(*) or count(field)
                        attr.aggregate = col.ColumnType == ColumnType.Wildcard ? AggregateType.count : AggregateType.countcolumn;
                        attr.aggregateSpecified = true;
                        break;

                    case "avg":
                    case "min":
                    case "max":
                    case "sum":
                        // All other aggregates can be applied directly
                        attr.aggregate = (AggregateType)Enum.Parse(typeof(AggregateType), func.FunctionName.Value.ToLower());
                        attr.aggregateSpecified = true;
                        break;

                    case "day":
                    case "week":
                    case "month":
                    case "quarter":
                    case "year":
                    case "fiscalperiod":
                    case "fiscalyear":
                        // Date groupings that have actually come from a rewritten DATEPART function
                        attr.dategrouping = (DateGroupingType)Enum.Parse(typeof(DateGroupingType), func.FunctionName.Value.ToLower());
                        attr.dategroupingSpecified = true;
                        break;

                    default:
                        // No other function calls are supported
                        throw new NotSupportedQueryFragmentException("Unhandled function", func);
                }

                // If we have either an aggregate or date grouping function, indicate that this is an aggregate query
                fetch.aggregate = true;
                fetch.aggregateSpecified = true;

                if (func.UniqueRowFilter == UniqueRowFilter.Distinct)
                {
                    // Handle `count(distinct col)` expressions
                    attr.distinct = FetchBoolType.@true;
                    attr.distinctSpecified = true;
                }

                if (String.IsNullOrEmpty(alias))
                {
                    // All aggregate and date grouping attributes must have an aggregate. If none is specified, auto-generate one
                    var aliasSuffix = attr.aggregateSpecified ? attr.aggregate.ToString() : attr.dategrouping.ToString();

                    alias = $"{attrName.Replace(".", "_")}_{aliasSuffix}";
                    var counter = 1;

                    while (cols.Contains(alias))
                    {
                        counter++;
                        alias = $"{attrName.Replace(".", "_")}_{aliasSuffix}_{counter}";
                    }
                }
            }

            attr.alias = alias;

            var addAttribute = true;

            if (table.Contains(i => i is allattributes))
            {
                // If we've already got an <all-attributes> element in this entity, either discard this <attribute> as it will be included
                // in the results anyway or generate an error if it has an alias as we can't combine <all-attributes> and an individual
                // <attribute>
                if (alias == null)
                    addAttribute = false;
                else
                    throw new NotSupportedQueryFragmentException("Cannot add aliased column and wildcard columns from same table", originalExpr);
            }

            // Don't add the attribute if we've already got it
            var newAttr = attr;
            var existingAttr = (table.Entity?.Items ?? table.LinkEntity?.Items ?? Array.Empty<object>()).OfType<FetchAttributeType>()
                .FirstOrDefault(existing =>
                    existing.name == newAttr.name &&
                    existing.aggregate == newAttr.aggregate &&
                    existing.aggregateSpecified == newAttr.aggregateSpecified &&
                    (existing.alias == newAttr.alias || matchAnyAlias) &&
                    existing.dategrouping == newAttr.dategrouping &&
                    existing.dategroupingSpecified == newAttr.dategroupingSpecified &&
                    existing.distinct == newAttr.distinct);

            if (existingAttr != null)
            {
                addAttribute = false;

                if (matchAnyAlias)
                    alias = existingAttr.alias;
            }

            if (addAttribute)
                table.AddItem(attr);
        }

        /// <summary>
        /// Converts the FROM clause of a query to FetchXML
        /// </summary>
        /// <param name="from">The FROM clause to convert</param>
        /// <param name="fetch">The FetchXML query to add the &lt;entity&gt; and &lt;link-entity&gt; elements to</param>
        /// <returns>A list of <see cref="EntityTable"/> instances providing easier access to the &lt;entity&gt; and &lt;link-entity&gt; elements</returns>
        private List<EntityTable> HandleFromClause(FromClause from, FetchXml.FetchType fetch)
        {
            // Check the clause includes only a single table or joins, we can't support
            // `SELECT * FROM table1, table2` syntax in FetchXML
            if (from.TableReferences.Count != 1)
                throw new NotSupportedQueryFragmentException("Unhandled SELECT FROM clause - only single table or qualified joins are supported", from);

            var tables = new List<EntityTable>();

            // Convert the table and recurse through the joins
            HandleFromClause(from.TableReferences[0], fetch, tables);

            return tables;
        }

        /// <summary>
        /// Converts a table or join within a FROM clause to FetchXML
        /// </summary>
        /// <param name="tableReference">The table or join to convert</param>
        /// <param name="fetch">The FetchXML wuery to add the &lt;entity&gt; and &lt;link-entity&gt; elements to</param>
        /// <param name="tables">The details of the tables added to the query so far</param>
        private void HandleFromClause(TableReference tableReference, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (tableReference is NamedTableReference namedTable)
            {
                // Handle a single table. First check we don't already have it in our list of tables
                var table = FindTable(namedTable, tables);

                if (table == null && fetch.Items == null)
                {
                    // This is the first table in our query, so add it to the root of the FetchXML
                    var entity = new FetchEntityType
                    {
                        name = namedTable.SchemaObject.BaseIdentifier.Value
                    };
                    fetch.Items = new object[] { entity };

                    try
                    {
                        table = new EntityTable(Metadata, entity) { Alias = namedTable.Alias?.Value };
                    }
                    catch (FaultException ex)
                    {
                        throw new NotSupportedQueryFragmentException(ex.Message, tableReference);
                    }

                    tables.Add(table);

                    // Apply the NOLOCK table hint. This isn't quite equivalent as it's applied at the table level in T-SQL but at
                    // the query level in FetchXML. Return an error if there's any other hints that FetchXML doesn't support
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
                // Handle a join. First check there aren't any join hints as FetchXML can't support them
                if (join.JoinHint != JoinHint.None)
                    throw new NotSupportedQueryFragmentException("Unsupported join hint", join);

                // Also can't join onto subqueries etc., only tables
                if (!(join.SecondTableReference is NamedTableReference table2))
                    throw new NotSupportedQueryFragmentException("Unsupported join table", join.SecondTableReference);

                // Recurse into the first table in the join
                HandleFromClause(join.FirstTableReference, fetch, tables);

                // Add a link-entity for the second table in the join
                var link = new FetchLinkEntityType
                {
                    name = table2.SchemaObject.BaseIdentifier.Value,
                    alias = table2.Alias?.Value ?? table2.SchemaObject.BaseIdentifier.Value
                };

                EntityTable linkTable;

                try
                {
                    linkTable = new EntityTable(Metadata, link);
                    tables.Add(linkTable);
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(ex.Message, table2);
                }
                
                // Parse the join condition to use as the filter on the link-entity
                var filter = new filter
                {
                    type = (filterType)2
                };
                
                ColumnReferenceExpression col1 = null;
                ColumnReferenceExpression col2 = null;
                Expression postFilter = null;
                HandleFilter(join.SearchCondition, filter, tables, linkTable, false, ref col1, ref col2, false, ref postFilter, null);

                // We need a join condition comparing a column in the link entity to one in another table
                if (col1 == null || col2 == null)
                    throw new NotSupportedQueryFragmentException("Missing join condition", join.SearchCondition);

                // If there were any other criteria in the join, add them as the filter
                if (filter.type != (filterType)2)
                {
                    // Because part of the filter has been separated out as the main join criteria, we can be left with an unnecessary nesting
                    // of filters. Strip this out now to simplify the final FetchXML
                    while (filter.Items != null && filter.Items.Length == 1 && filter.Items[0] is filter)
                        filter = (filter)filter.Items[0];

                    linkTable.AddItem(filter);
                }

                // Check the join type is supported
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

                // Check what other entity is referenced in the join criteria. Exactly one of the columns being compared must be from the
                // new join table and the other must be from another table that is already part of the query
                ColumnReferenceExpression linkFromAttribute;
                ColumnReferenceExpression linkToAttribute;

                GetColumnTableAlias(col1, tables, out var lhs);
                GetColumnTableAlias(col2, tables, out var rhs);

                if (lhs == null || rhs == null)
                    throw new NotSupportedQueryFragmentException("Join condition does not reference previous table", join.SearchCondition);

                // Normalize the join so we know which attribute is the "from" and which is the "to"
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
                // No other table references are supported, e.g. CTE, subquery
                throw new NotSupportedQueryFragmentException("Unhandled SELECT FROM clause", tableReference);
            }
        }

        /// <summary>
        /// Find a table within the query data source
        /// </summary>
        /// <param name="namedTable">The table reference to find</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <returns>The table in the <paramref name="tables"/> list that matches the <paramref name="namedTable"/> reference, or <c>null</c> if the table cannot be found</returns>
        private EntityTable FindTable(NamedTableReference namedTable, List<EntityTable> tables)
        {
            if (namedTable.Alias != null)
            {
                // Search by alias first if we have one. Find the existing table with the same alias, and if we can find one check that
                // it has the expected entity name
                var aliasedTable = tables.SingleOrDefault(t => t.Alias.Equals(namedTable.Alias.Value, StringComparison.OrdinalIgnoreCase));

                if (aliasedTable == null)
                    return null;

                if (!aliasedTable.EntityName.Equals(namedTable.SchemaObject.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedQueryFragmentException("Duplicate table alias", namedTable);

                return aliasedTable;
            }

            // If we don't have an alias, search by name. Match against the table alias where specified, then the table name
            var table = tables.SingleOrDefault(t => (t.Alias ?? t.EntityName).Equals(namedTable.SchemaObject.BaseIdentifier.Value, StringComparison.OrdinalIgnoreCase));

            return table;
        }

        /// <summary>
        /// Finds a table within the query data source
        /// </summary>
        /// <param name="name">The name or alias of the table to find</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="fragment">The SQL fragment that the table reference comes from</param>
        /// <returns>The matching table, or <c>null</c> if the table cannot be found</returns>
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

        /// <summary>
        /// Gets the alias or name of the entity that is referenced by a column
        /// </summary>
        /// <param name="col">The column reference to find the source table for</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="table">The full details of the table that the column comes from</param>
        /// <returns>The alias or name of the matching <paramref name="table"/></returns>
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

                if (table == null)
                    throw new NotSupportedQueryFragmentException("Unknown table " + col.MultiPartIdentifier.Identifiers[0].Value, col);

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
                    .Where(t => (t.Entity?.Items ?? t.LinkEntity?.Items)?.OfType<FetchAttributeType>()?.Any(attr => attr.alias?.Equals(col.MultiPartIdentifier.Identifiers[0].Value, StringComparison.OrdinalIgnoreCase) == true) == true)
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

            var list = new List<object>(items)
            {
                item
            };
            return list.ToArray();
        }
    }
}
