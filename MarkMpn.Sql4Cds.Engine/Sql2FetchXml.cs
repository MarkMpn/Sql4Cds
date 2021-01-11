using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Globalization;
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
                EntityName = entity.name.ToLower();
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
                EntityName = link.name.ToLower();
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
            /// Indicates if this table has been added by the engine and shouldn't be used for column name resolution
            /// </summary>
            public bool Hidden { get; set; }

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
            /// Gets the list of items within the entity or link-entity
            /// </summary>
            /// <returns>The list of child items</returns>
            internal object[] GetItems()
            {
                if (Entity?.Items != null)
                    return Entity.Items;

                if (LinkEntity?.Items != null)
                    return LinkEntity.Items;

                return Array.Empty<object>();
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
            Metadata = new MetaMetadataCache(metadata);
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
        /// Indicates if the CDS TDS endpoint can be used as a fallback if a query cannot be converted to FetchXML
        /// </summary>
        public bool TDSEndpointAvailable { get; set; }

        /// <summary>
        /// Indicates if the CDS TDS endpoint should be used wherever possible
        /// </summary>
        public bool ForceTDSEndpoint { get; set; }

        /// <summary>
        /// Indicates if column comparison conditions are supported
        /// </summary>
        public bool ColumnComparisonAvailable { get; set; }

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
            script.Accept(new ReplacePrimaryFunctionsVisitor());

            // Convert each statement in turn to the appropriate query type
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    var index = statement.StartOffset;
                    var length = statement.ScriptTokenStream[statement.LastTokenIndex].Offset + statement.ScriptTokenStream[statement.LastTokenIndex].Text.Length - index;
                    var originalSql = statement.ToSql();
                    string sqlStatement = null;

                    if (TDSEndpointAvailable)
                    {
                        var statementFragment = dom.Parse(new StringReader(statement.ToSql()), out _);
                        var statementScript = (TSqlScript)statementFragment;
                        statementScript.Accept(new AddDefaultTableAliasesVisitor());
                        statementScript.ScriptTokenStream = null;
                        sqlStatement = statementScript.ToSql();
                    }

                    Query query;

                    if (statement is SelectStatement select)
                        query = ConvertSelectStatement(select, false);
                    else if (statement is UpdateStatement update)
                        query = ConvertUpdateStatement(update);
                    else if (statement is DeleteStatement delete)
                        query = ConvertDeleteStatement(delete);
                    else if (statement is InsertStatement insert)
                        query = ConvertInsertStatement(insert);
                    else if (statement is ExecuteAsStatement impersonate)
                        query = ConvertExecuteAsStatement(impersonate);
                    else if (statement is RevertStatement revert)
                        query = ConvertRevertStatement(revert);
                    else
                        throw new NotSupportedQueryFragmentException("Unsupported statement", statement);

                    query.Sql = originalSql;
                    query.Index = index;
                    query.Length = length;
                    queries.Add(query);
                }
            }

            return queries.ToArray();
        }

        /// <summary>
        /// Converts a REVERT statement from SQL
        /// </summary>
        /// <param name="revert">The parsed REVERT statement</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertRevertStatement(RevertStatement revert)
        {
            // Check for any DOM elements we don't support converting
            if (revert.Cookie != null)
                throw new NotSupportedQueryFragmentException("Unhandled impersonation cookie", revert.Cookie);

            return new RevertQuery();
        }

        /// <summary>
        /// Converts an EXECUTE AS [USER | LOGIN] statement from SQL
        /// </summary>
        /// <param name="impersonate">The parsed EXECUTE AS statement</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertExecuteAsStatement(ExecuteAsStatement impersonate)
        {
            // Check for any DOM elements we don't support converting
            if (impersonate.Cookie != null)
                throw new NotSupportedQueryFragmentException("Unhandled impersonation cookie", impersonate.Cookie);

            if (impersonate.WithNoRevert)
                throw new NotSupportedQueryFragmentException("Unhandled WITH NO REVERT option", impersonate);

            if (impersonate.ExecuteContext.Kind != ExecuteAsOption.Login &&
                impersonate.ExecuteContext.Kind != ExecuteAsOption.User)
                throw new NotSupportedQueryFragmentException("Unhandled impersonation type", impersonate.ExecuteContext);

            if (!(impersonate.ExecuteContext.Principal is StringLiteral user))
                throw new NotSupportedQueryFragmentException("Unhandled username variable", impersonate.ExecuteContext.Principal);

            var username = user.Value;

            return new ImpersonateQuery(username);
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

            var selectQuery = ConvertSelectStatement(qry, false);

            // Check that the number of columns for the source query and target columns match
            if (columns.Count != selectQuery.ColumnSet.Length)
                throw new NotSupportedQueryFragmentException("Number of columns generated by SELECT does not match number of columns in INSERT", select);

            // Populate the final query based on the converted SELECT query
            var query = new InsertSelect
            {
                LogicalName = target,
                Source = selectQuery,
                Mappings = new Dictionary<string, string>(),
            };

            var meta = Metadata[target];

            for (var i = 0; i < columns.Count; i++)
            {
                var attr = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(columns[i].MultiPartIdentifier.Identifiers.Last().Value, StringComparison.OrdinalIgnoreCase));

                if (attr == null)
                    throw new NotSupportedQueryFragmentException("Unknown attribute name", columns[i]);

                if (meta.IsIntersect != true && attr.IsValidForCreate == false)
                    throw new NotSupportedQueryFragmentException("Column cannot be set on INSERT", columns[i]);

                query.Mappings[selectQuery.ColumnSet[i]] = attr.LogicalName;
            }

            return query;
        }

        /// <summary>
        /// Convert an INSERT INTO ... VALUES ... query
        /// </summary>
        /// <param name="target">The entity to insert the values into</param>
        /// <param name="columns">The list of columns within the <paramref name="target"/> entity to populate with the supplied <paramref name="values"/></param>
        /// <param name="source">The values to insert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private Query ConvertInsertValuesStatement(string target, IList<ColumnReferenceExpression> columns, ValuesInsertSource source)
        {
            // Get the metadata for the target entity
            var rowValues = new List<IDictionary<string, object>>();
            var meta = Metadata[target];

            // Convert the supplied values to the appropriate type for the attribute it is to be inserted into
            foreach (var row in source.RowValues)
            {
                var values = new Dictionary<string, object>();

                if (row.ColumnValues.Count != columns.Count)
                    throw new NotSupportedQueryFragmentException("Number of values does not match number of columns", row);

                for (var i = 0; i < columns.Count; i++)
                {
                    var columnName = columns[i].MultiPartIdentifier.Identifiers.Last().Value;
                    var attr = meta.Attributes.SingleOrDefault(a => a.LogicalName == columnName);

                    if (attr == null)
                        throw new NotSupportedQueryFragmentException("Unknown attribute name", columns[i]);

                    if (attr.IsValidForCreate == false)
                        throw new NotSupportedQueryFragmentException("Column cannot be set on INSERT", columns[i]);

                    if (row.ColumnValues[i] is Literal literal)
                    {
                        try
                        {
                            values[columnName] = ConvertAttributeValueType(meta, columnName, literal.Value);
                        }
                        catch (FormatException)
                        {
                            throw new NotSupportedQueryFragmentException("Invalid value format", literal);
                        }
                    }
                    else
                    {
                        var attrType = GetAttributeType(attr, true);
                        var expr = CompileScalarExpression(row.ColumnValues[i], new List<EntityTable>(), null, attrType, out _);
                        values[columnName] = expr(null);
                    }
                }

                rowValues.Add(values);
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
        private DeleteQuery ConvertDeleteStatement(DeleteStatement delete)
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

            if (ForceTDSEndpoint)
                return ConvertDeleteStatementTDS(delete, target);

            try
            {
                return ConvertDeleteStatementFetchXml(delete, target);
            }
            catch (NotSupportedQueryFragmentException)
            {
                if (!TDSEndpointAvailable)
                    throw;

                return ConvertDeleteStatementTDS(delete, target);
            }
        }

        private DeleteQuery ConvertDeleteStatementFetchXml(DeleteStatement delete, NamedTableReference target)
        {
            // Convert the FROM, TOP and WHERE clauses from the query to identify the records to delete.
            // Each record can only be deleted once, so apply the DISTINCT option as well
            var fetch = new FetchXml.FetchType
            {
                distinct = true,
                distinctSpecified = true
            };
            var extensions = new List<IQueryExtension>();
            var tables = HandleFromClause(delete.DeleteSpecification.FromClause, fetch);
            HandleWhereClause(delete.DeleteSpecification.WhereClause, tables, extensions);
            HandleTopClause(delete.DeleteSpecification.TopRowFilter, fetch, extensions);

            // To delete a record we need the primary key field of the target entity
            // For intersect entities we need the two foreign key fields instead
            var table = FindTable(target, tables);
            var meta = Metadata[table.EntityName];

            if (table.EntityName == "listmember")
            {
                table.AddItem(new FetchAttributeType { name = "listid" });
                table.AddItem(new FetchAttributeType { name = "entityid" });

                table.AddItem(new FetchOrderType { attribute = "listid" });
                table.AddItem(new FetchOrderType { attribute = "entityid" });
            }
            else if (meta.IsIntersect == true)
            {
                var relationship = meta.ManyToManyRelationships.Single();
                table.AddItem(new FetchAttributeType { name = relationship.Entity1IntersectAttribute });
                table.AddItem(new FetchAttributeType { name = relationship.Entity2IntersectAttribute });

                table.AddItem(new FetchOrderType { attribute = relationship.Entity1IntersectAttribute });
                table.AddItem(new FetchOrderType { attribute = relationship.Entity2IntersectAttribute });
            }
            else
            {
                table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });

                table.AddItem(new FetchOrderType { attribute = meta.PrimaryIdAttribute });
            }

            var cols = table.GetItems().OfType<FetchAttributeType>().Select(a => a.name).ToArray();

            if (table.Entity == null)
            {
                // If we're deleting from a table other than the first one in the joins, we need to include the table name/alias
                // prefix in the column list
                for (var i = 0; i < cols.Length; i++)
                    cols[i] = (table.Alias ?? table.EntityName) + "." + cols[i];
            }

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var t in tables)
                t.Sort();

            // Return the final query
            var query = new DeleteQuery
            {
                FetchXml = fetch,
                EntityName = table.EntityName,
                IdColumns = cols,
                AllPages = fetch.page == null && fetch.top == null,
                HasWhere = delete.DeleteSpecification.WhereClause != null
            };

            foreach (var extension in extensions)
                query.Extensions.Add(extension);

            return query;
        }

        private DeleteQuery ConvertDeleteStatementTDS(DeleteStatement delete, NamedTableReference target)
        {
            // We need to select the unique identifier of the entity being deleted. Target of update might be
            // an alias for a table in the FROM clause.
            var visitor = new UpdateTargetVisitor(target.SchemaObject.BaseIdentifier.Value);
            delete.DeleteSpecification.FromClause.Accept(visitor);
            var metadata = Metadata[visitor.TargetEntityName];

            var select = new SelectStatement
            {
                QueryExpression = new QuerySpecification
                {
                    FromClause = delete.DeleteSpecification.FromClause,
                    TopRowFilter = delete.DeleteSpecification.TopRowFilter,
                    WhereClause = delete.DeleteSpecification.WhereClause,
                    UniqueRowFilter = UniqueRowFilter.Distinct
                },
                ScriptTokenStream = null
            };

            var idColumns = new List<string>();

            if (metadata.LogicalName == "listmember")
            {
                idColumns.Add("entityid");
                idColumns.Add("listid");
            }
            else if (metadata.IsIntersect == true)
            {
                var relationship = metadata.ManyToManyRelationships.Single();
                idColumns.Add(relationship.Entity1IntersectAttribute);
                idColumns.Add(relationship.Entity2IntersectAttribute);
            }
            else
            {
                idColumns.Add(metadata.PrimaryIdAttribute);
            }

            foreach (var idColumn in idColumns)
            {
                ((QuerySpecification)select.QueryExpression).SelectElements.Add(new SelectScalarExpression
                {
                    Expression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = visitor.TargetAliasName },
                                new Identifier { Value = idColumn }
                            }
                        }
                    },
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = new Identifier { Value = idColumn }
                    }
                });
            }

            var query = new DeleteQuery
            {
                EntityName = metadata.LogicalName,
                IdColumns = idColumns.ToArray(),
                TSql = select.ToSql(),
                HasWhere = delete.DeleteSpecification.WhereClause != null
            };

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

            if (ForceTDSEndpoint)
                return ConvertUpdateStatementTDS(update, target);

            try
            {
                return ConvertUpdateStatementFetchXml(update, target);
            }
            catch (NotSupportedQueryFragmentException)
            {
                // If we can use the TSQL endpoint, rewrite the query as a SELECT to get the required values
                if (!TDSEndpointAvailable)
                    throw;

                return ConvertUpdateStatementTDS(update, target);
            }
        }

        /// <summary>
        /// Converts an UPDATE query to FetchXML
        /// </summary>
        /// <param name="update">The SQL query to convert</param>
        /// <param name="target">The name of the entity to be updated</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private UpdateQuery ConvertUpdateStatementFetchXml(UpdateStatement update, NamedTableReference target)
        {
            // Convert the FROM, TOP and WHERE clauses from the query to identify the records to update.
            // Each record can only be updated once, so apply the DISTINCT option as well
            var fetch = new FetchXml.FetchType
            {
                distinct = true,
                distinctSpecified = true
            };
            var extensions = new List<IQueryExtension>();
            var tables = HandleFromClause(update.UpdateSpecification.FromClause, fetch);
            HandleWhereClause(update.UpdateSpecification.WhereClause, tables, extensions);
            HandleTopClause(update.UpdateSpecification.TopRowFilter, fetch, extensions);

            var table = FindTable(target, tables);

            if (table == null)
            {
                // Invalid table name. See how helpful we can make the error message
                var tableName = target.SchemaObject.BaseIdentifier.Value;
                var matchingTables = tables.Where(t => t.EntityName.Equals(tableName, StringComparison.OrdinalIgnoreCase)).ToList();

                if (matchingTables.Count == 0)
                    throw new NotSupportedQueryFragmentException("Table does not appear in FROM clause", target);

                if (matchingTables.Count > 1)
                    throw new NotSupportedQueryFragmentException("Ambiguous table name", target);

                table = matchingTables[0];
            }

            var meta = Metadata[table.EntityName];

            // Get the details of what fields should be updated to what
            var updates = HandleSetClause(update.UpdateSpecification.SetClauses, tables, meta);

            // To update a record we need the primary key field of the target entity
            table.AddItem(new FetchAttributeType { name = meta.PrimaryIdAttribute });
            var cols = new[] { meta.PrimaryIdAttribute };
            if (table.Entity == null)
                cols[0] = (table.Alias ?? table.EntityName) + "." + cols[0];

            // Sort by this column to avoid problems with DISTINCT
            table.AddItem(new FetchOrderType { attribute = meta.PrimaryIdAttribute });

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var t in tables)
                t.Sort();

            // Return the final query
            var query = new UpdateQuery
            {
                FetchXml = fetch,
                EntityName = table.EntityName,
                IdColumn = cols[0],
                Updates = updates,
                AllPages = fetch.page == null && fetch.top == null,
                HasWhere = update.UpdateSpecification.WhereClause != null
            };

            foreach (var extension in extensions)
                query.Extensions.Add(extension);

            return query;
        }

        /// <summary>
        /// Converts an UPDATE query using the TDS endpoint
        /// </summary>
        /// <param name="update">The SQL query to convert</param>
        /// <param name="target">The name of the entity to be updated</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private UpdateQuery ConvertUpdateStatementTDS(UpdateStatement update, NamedTableReference target)
        {
            // We need to select the unique identifier of the entity being updated. Target of update might be
            // an alias for a table in the FROM clause.
            var visitor = new UpdateTargetVisitor(target.SchemaObject.BaseIdentifier.Value);
            update.UpdateSpecification.FromClause.Accept(visitor);
            var metadata = Metadata[visitor.TargetEntityName];

            var select = new SelectStatement
            {
                QueryExpression = new QuerySpecification
                {
                    SelectElements =
                        {
                            new SelectScalarExpression
                            {
                                Expression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier{ Value = visitor.TargetAliasName },
                                            new Identifier{ Value = metadata.PrimaryIdAttribute }
                                        }
                                    }
                                },
                                ColumnName = new IdentifierOrValueExpression
                                {
                                    Identifier = new Identifier { Value = metadata.PrimaryIdAttribute }
                                }
                            }
                        },
                    FromClause = update.UpdateSpecification.FromClause,
                    WhereClause = update.UpdateSpecification.WhereClause,
                    TopRowFilter = update.UpdateSpecification.TopRowFilter
                },
                ScriptTokenStream = null
            };

            // Add each column being set to the SELECT statement
            var columns = new Dictionary<string, AssignmentSetClause>(StringComparer.OrdinalIgnoreCase);

            foreach (var set in update.UpdateSpecification.SetClauses)
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

                if (attr.IsValidForUpdate == false)
                    throw new NotSupportedQueryFragmentException("Column is not updateable", assign.Column);

                ((QuerySpecification)select.QueryExpression).SelectElements.Add(new SelectScalarExpression
                {
                    Expression = assign.NewValue,
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = assign.Column.MultiPartIdentifier.Identifiers[0]
                    }
                });

                columns.Add(attr.LogicalName, assign);
            }

            // Most columns map to a single attribute, but polymorphic lookup attributes are formed from two columns. Merge
            // the two values into a single attribute value.
            var updates = new Dictionary<string, Func<Entity, object>>();

            foreach (var attr in metadata.Attributes)
            {
                if (attr.AttributeOf != null)
                    continue;

                if (!columns.ContainsKey(attr.LogicalName))
                    continue;

                if (attr is LookupAttributeMetadata lookup)
                {
                    updates[attr.LogicalName] = e =>
                    {
                        if (!e.Contains(attr.LogicalName))
                            return null;

                        var id = e[attr.LogicalName];

                        if (id == null)
                            return null;

                        if (id is string s)
                            id = new Guid(s);
                        else if (!(id is Guid))
                            throw new NotSupportedQueryFragmentException($"Cannot convert value '{id}' of type '{id.GetType()}' to '{attr.AttributeType}' for attribute {attr.LogicalName}", columns[attr.LogicalName]);

                        string logicalName;

                        if (e.Contains(attr.LogicalName + "type"))
                        {
                            var logicalNameValue = e[attr.LogicalName + "type"];

                            if (logicalNameValue is string)
                                logicalName = (string)logicalNameValue;
                            else if (logicalNameValue is int otc)
                                logicalName = Metadata[otc].LogicalName;
                            else
                                throw new NotSupportedQueryFragmentException($"Cannot convert value '{logicalNameValue}' of type '{logicalNameValue.GetType()}' to 'string' for attribute {attr.LogicalName}", columns[attr.LogicalName + "type"]);
                        }
                        else if (lookup.Targets.Length == 1)
                        {
                            logicalName = lookup.Targets[0];
                        }
                        else
                        {
                            throw new NotSupportedQueryFragmentException("Polymorphic lookup columns must have the corresponding type column set as well", columns[attr.LogicalName]);
                        }

                        return new EntityReference(logicalName, (Guid)id);
                    };
                }
                else
                {
                    updates[attr.LogicalName] = e =>
                    {
                        if (!e.Contains(attr.LogicalName))
                            return null;

                        var value = e[attr.LogicalName];
                        value = ConvertAttributeValueType(metadata, attr.LogicalName, value);

                        return value;
                    };
                }
            }

            var query = new UpdateQuery
            {
                EntityName = visitor.TargetEntityName,
                IdColumn = metadata.PrimaryIdAttribute,
                Updates = updates,
                TSql = select.ToSql(),
                HasWhere = update.UpdateSpecification.WhereClause != null
            };

            return query;
        }

        /// <summary>
        /// Converts an attribute value to the appropriate type for INSERT and UPDATE queries
        /// </summary>
        /// <param name="metadata">The metadata of the entity being affected</param>
        /// <param name="attrName">The name of the attribute</param>
        /// <param name="value">The value for the attribute</param>
        /// <returns>The <paramref name="value"/> converted to the type appropriate for the <paramref name="attrName"/></returns>
        internal static object ConvertAttributeValueType(EntityMetadata metadata, string attrName, object value)
        {
            // Don't care about types for nulls
            if (value == null)
                return null;

            // Find the correct attribute
            var attr = metadata.Attributes.SingleOrDefault(a => a.LogicalName == attrName);

            if (attr == null)
                throw new NotSupportedException("Unknown attribute " + attrName);

            var str = value as string;
            var b = value as bool?;
            var i = value as int?;
            var l = value as long?;
            var dec = value as decimal?;
            var dbl = value as double?;
            var g = value as Guid?;
            var dt = value as DateTime?;
            var er = value as EntityReference;
            var m = value as Money;
            var osv = value as OptionSetValue;
            var osvc = value as OptionSetValueCollection;

            // Handle the conversion for each attribute type
            switch (attr.AttributeType)
            {
                case AttributeTypeCode.BigInt:
                    if (str != null)
                        return Int64.Parse(str, CultureInfo.InvariantCulture);
                    if (b != null)
                        return b.Value ? 1L : 0L;
                    if (i != null)
                        return (long)i.Value;
                    if (l != null)
                        return l.Value;
                    if (m != null)
                        return (long)m.Value;
                    if (osv != null)
                        return (long)osv.Value;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Boolean:
                    if (str == "0")
                        return false;
                    if (str == "1")
                        return true;
                    if (b != null)
                        return b.Value;
                    if (i != null)
                        return i.Value != 0;
                    if (l != null)
                        return l.Value != 0;
                    if (m != null)
                        return m.Value != 0;
                    if (osv != null)
                        return osv.Value != 0;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.DateTime:
                    if (str != null)
                        return DateTime.Parse(str, CultureInfo.InvariantCulture);
                    if (dt != null)
                        return dt.Value;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Decimal:
                    if (str != null)
                        return Decimal.Parse(str, CultureInfo.InvariantCulture);
                    if (i != null)
                        return (decimal)i.Value;
                    if (l != null)
                        return (decimal)l.Value;
                    if (dec != null)
                        return dec.Value;
                    if (dbl != null)
                        return (decimal)dbl.Value;
                    if (m != null)
                        return (decimal)m.Value;
                    if (osv != null)
                        return (decimal)osv.Value;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Double:
                    if (str != null)
                        return Double.Parse(str, CultureInfo.InvariantCulture);
                    if (i != null)
                        return (double)i.Value;
                    if (l != null)
                        return (double)l.Value;
                    if (dec != null)
                        return (double)dec.Value;
                    if (dbl != null)
                        return dbl.Value;
                    if (m != null)
                        return (double)m.Value;
                    if (osv != null)
                        return (double)osv.Value;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Integer:
                    if (str != null)
                        return Int32.Parse(str, CultureInfo.InvariantCulture);
                    if (i != null)
                        return i.Value;
                    if (l != null)
                        return (int)l.Value;
                    if (dec != null)
                        return (int)dec.Value;
                    if (dbl != null)
                        return (int)dbl.Value;
                    if (m != null)
                        return (int)m.Value;
                    if (osv != null)
                        return osv.Value;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Lookup:
                case AttributeTypeCode.Customer:
                case AttributeTypeCode.Owner:
                    if (er != null)
                        return er;
                    if (str != null)
                        g = Guid.Parse(str);
                    if (g == null)
                        throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");
                    var targets = ((LookupAttributeMetadata)attr).Targets;
                    if (targets.Length != 1)
                        throw new NotSupportedException($"Cannot use guid value for polymorphic lookup attribute {attrName}. Use CREATELOOKUP(logicalname, guid) function instead");
                    return new EntityReference(targets[0], g.Value);

                case AttributeTypeCode.Memo:
                case AttributeTypeCode.String:
                    return value.ToString();

                case AttributeTypeCode.Money:
                    if (str != null)
                        return new Money(Decimal.Parse(str, CultureInfo.InvariantCulture));
                    if (i != null)
                        return new Money(i.Value);
                    if (l != null)
                        return new Money(l.Value);
                    if (dec != null)
                        return new Money(dec.Value);
                    if (dbl != null)
                        return new Money((decimal)dbl.Value);
                    if (m != null)
                        return m;
                    if (osv != null)
                        return new Money(osv.Value);
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Picklist:
                case AttributeTypeCode.State:
                case AttributeTypeCode.Status:
                    if (str != null)
                        return new OptionSetValue(Int32.Parse(str, CultureInfo.InvariantCulture));
                    if (i != null)
                        return new OptionSetValue(i.Value);
                    if (l != null)
                        return new OptionSetValue((int)l.Value);
                    if (osv != null)
                        return osv;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Uniqueidentifier:
                    if (str != null)
                        return Guid.Parse(str);
                    if (g != null)
                        return g.Value;
                    if (er != null)
                        return er.Id;
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

                case AttributeTypeCode.Virtual:
                    if (attr.AttributeTypeName.Value == "MultiSelectPicklistType")
                    {
                        if (osvc != null)
                            return osvc;
                        if (osv != null)
                            return new OptionSetValueCollection(new[] { osv });
                        if (i != null)
                            return new OptionSetValueCollection(new[] { new OptionSetValue(i.Value) });
                        if (str != null)
                        {
                            var parts = str.Split(',');
                            osvc = new OptionSetValueCollection();
                            foreach(var part in parts)
                            {
                                if (!Int32.TryParse(part.Trim(), out var val))
                                    throw new InvalidOperationException($"Cannot convert value '{part}' of type '{part.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");
                                osvc.Add(new OptionSetValue(val));
                            }
                            return osvc;
                        }
                    }
                    throw new InvalidOperationException($"Cannot convert value '{value}' of type '{value.GetType()}' to '{attr.AttributeType}' for attribute {attrName}");

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

                    if (attr.IsValidForUpdate == false)
                        throw new NotSupportedQueryFragmentException("Column is not updateable", assign.Column);

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
                            try
                            {
                                var value = ConvertAttributeValueType(metadata, attrName, literal.Value);
                                return new { Key = attrName, Value = (Func<Entity, object>)(e => value) };
                            }
                            catch (FormatException)
                            {
                                throw new NotSupportedQueryFragmentException("Invalid value format", literal);
                            }
                        }
                    }
                    else
                    {
                        // Handle updates to the value from another field
                        // Ensure the source field is included in the query
                        var targetType = GetAttributeType(attr, true);
                        return new { Key = attrName, Value = CompileScalarExpression(assign.NewValue, tables, null, targetType, out _) };
                    }
                })
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        /// <summary>
        /// Converts a scalar SQL expression to a compiled function that evaluates the final value of the expression for a given entity.
        /// </summary>
        /// <param name="expr">The <see cref="ScalarExpression"/> to convert</param>
        /// <param name="tables">The tables to use as the data source for the expression</param>
        /// <param name="calculatedFields">Any calculated fields that will be created when running the query</param>
        /// <param name="expression">The <see cref="System.Linq.Expressions.Expression"/> that the <paramref name="expr"/> was converted to before compilation</param>
        /// <param name="targetType">The type of value to be returned by the function</param>
        /// <returns>A compiled expresson that evaluates the supplied <paramref name="expr"/> for a given entity</returns>
        private Func<Entity,object> CompileScalarExpression(ScalarExpression expr, List<EntityTable> tables, IDictionary<string, Type> calculatedFields, Type targetType, out Expression expression)
        {
            var constantValue = true;
            expression = ConvertScalarExpression(expr, tables, calculatedFields, _param, ref constantValue);

            if (expression.Type == typeof(Guid?) && targetType == typeof(EntityReference) && expr is ColumnReferenceExpression col)
            {
                // We're trying to convert a primary key to a lookup field, so get the logical name from the source
                GetColumnTableAlias(col, tables, out var table);
                var logicalName = table.EntityName;

                expression = Expr.Call(() => ExpressionFunctions.CreatePrimaryKeyLookup(Expr.Arg<string>(), Expr.Arg<Guid?>()), Expression.Constant(logicalName), expression);
            }

            var finalExpr = Expr.Convert(expression, targetType);

            if (targetType.IsValueType)
                finalExpr = Expr.Convert(expression, typeof(object));

            return Expression.Lambda<Func<Entity, object>>(finalExpr, _param).Compile();
        }

        /// <summary>
        /// Converts a scalar SQL expression to a compiled function that evaluates the final value of the expression for a given entity.
        /// </summary>
        /// <param name="expr">The <see cref="ScalarExpression"/> to convert</param>
        /// <param name="tables">The tables to use as the data source for the expression</param>
        /// <param name="calculatedFields">Any calculated fields that will be created when running the query</param>
        /// <param name="param">The parameter that holds the entity the expression is being evaluated for</param>
        /// <returns>A compiled expression that evaluates the supplied <paramref name="expr"/> for a given entity</returns>
        /// <remarks>
        /// This method will ensure any attributes required by the <paramref name="expr"/> are added to the query.
        /// </remarks>
        private Expression ConvertScalarExpression(ScalarExpression expr, List<EntityTable> tables, IDictionary<string, Type> calculatedFields, ParameterExpression param)
        {
            var constantValue = true;
            return ConvertScalarExpression(expr, tables, calculatedFields, param, ref constantValue);
        }

        private bool IsConstantValueExpression(ScalarExpression expr, List<EntityTable> tables, IDictionary<string, Type> calculatedFields, ParameterExpression param, out Literal literal)
        {
            var constantValue = true;
            var converted = ConvertScalarExpression(expr, tables, calculatedFields, param, ref constantValue);

            if (constantValue)
            {
                if (converted.Type.IsValueType)
                    converted = Expr.Convert(converted, typeof(object));

                var lambda = Expression.Lambda<Func<Entity, object>>(converted, _param).Compile();

                var value = lambda(null);

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
                {
                    literal = null;
                    constantValue = false;
                }
            }
            else
            {
                literal = null;
            }

            return constantValue;
        }

        /// <summary>
        /// Converts a scalar SQL expression to a compiled function that evaluates the final value of the expression for a given entity.
        /// </summary>
        /// <param name="expr">The <see cref="ScalarExpression"/> to convert</param>
        /// <param name="tables">The tables to use as the data source for the expression</param>
        /// <param name="calculatedFields">Any calculated fields that will be created when running the query</param>
        /// <param name="param">The parameter that holds the entity the expression is being evaluated for</param>
        /// <param name="constantValue">Indicates if the value of the expression is dependant on the <paramref name="param"/> (<c>false</c>) or not (<c>true</c>)</param>
        /// <returns>A compiled expression that evaluates the supplied <paramref name="expr"/> for a given entity</returns>
        /// <remarks>
        /// This method will ensure any attributes required by the <paramref name="expr"/> are added to the query.
        /// </remarks>
        private Expression ConvertScalarExpression(ScalarExpression expr, List<EntityTable> tables, IDictionary<string,Type> calculatedFields, ParameterExpression param, ref bool constantValue)
        {
            if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression binary)
            {
                // Binary operator - convert the two operands to expressions first
                var lhs = ConvertScalarExpression(binary.FirstExpression, tables, calculatedFields, param, ref constantValue);
                var rhs = ConvertScalarExpression(binary.SecondExpression, tables, calculatedFields, param, ref constantValue);

                // If either operand is a Nullable<T>, get the inner Value to use for the main operator
                var lhsValue = lhs;
                var rhsValue = rhs;

                if (lhsValue.Type.IsGenericType && lhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    lhsValue = Expression.Property(lhsValue, lhsValue.Type.GetProperty("Value"));

                if (rhsValue.Type.IsGenericType && rhsValue.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    rhsValue = Expression.Property(rhsValue, rhsValue.Type.GetProperty("Value"));

                // If one value is a decimal and another isn't, convert them
                if (lhsValue.Type == typeof(decimal) && (rhsValue.Type == typeof(int) || rhsValue.Type == typeof(double)))
                    rhsValue = Expression.Convert(rhsValue, typeof(decimal));

                if (rhsValue.Type == typeof(decimal) && (lhsValue.Type == typeof(int) || lhsValue.Type == typeof(double)))
                    lhsValue = Expression.Convert(lhsValue, typeof(decimal));

                Expression coreOperator;

                switch (binary.BinaryExpressionType)
                {
                    case BinaryExpressionType.Add:
                        if (lhs.Type == typeof(string))
                            coreOperator = Expression.Add(lhsValue, rhsValue, typeof(string).GetMethod("Concat", new[] { typeof(object), typeof(object) }));
                        else
                            coreOperator = Expression.Add(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.BitwiseAnd:
                        coreOperator = Expression.And(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.BitwiseOr:
                        coreOperator = Expression.Or(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.BitwiseXor:
                        coreOperator = Expression.ExclusiveOr(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.Divide:
                        coreOperator = Expression.Divide(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.Modulo:
                        coreOperator = Expression.Modulo(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.Multiply:
                        coreOperator = Expression.Multiply(lhsValue, rhsValue);
                        break;

                    case BinaryExpressionType.Subtract:
                        coreOperator = Expression.Subtract(lhsValue, rhsValue);
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

                if (targetType.IsValueType)
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
                var attrName = col.MultiPartIdentifier.Identifiers.Last().Value;
                Type type;
                
                if (col.MultiPartIdentifier.Identifiers.Count != 1 || calculatedFields == null || !calculatedFields.TryGetValue(col.MultiPartIdentifier.Identifiers[0].Value, out type))
                {
                    // Check where this attribute should be taken from
                    var tableAlias = GetColumnTableAlias(col, tables, out var table);
                    attrName = GetColumnAttribute(table, col);

                    var attrMetadata = table.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                    if (attrMetadata == null)
                        throw new NotSupportedQueryFragmentException("Unknown attribute", expr);

                    if (attrMetadata.AttributeType == null)
                        throw new NotSupportedQueryFragmentException("Unknown attribute type", expr);

                    var physicalAttrName = attrName;

                    if (!String.IsNullOrEmpty(attrMetadata?.AttributeOf))
                        physicalAttrName = attrMetadata.AttributeOf;
                    
                    // Check if the attribute needs to be added to the query
                    if (!table.GetItems().Any(i => i is allattributes) &&
                        !table.GetItems().Any(i => i is FetchAttributeType attr && attr.name == physicalAttrName && attr.alias == null && !attr.aggregateSpecified && !attr.dategroupingSpecified))
                    {
                        table.AddItem(new FetchAttributeType { name = physicalAttrName });
                    }

                    type = GetAttributeType(attrMetadata, false);

                    if (type == null)
                        throw new NotSupportedQueryFragmentException("Unknown attribute type", expr);

                    if (tableAlias != null)
                        attrName = tableAlias + "." + attrName;
                }

                constantValue = false;

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
                try
                {
                    return Expression.Constant(ConvertLiteralValue(literal));
                }
                catch (FormatException)
                {
                    throw new NotSupportedQueryFragmentException("Invalid value format", literal);
                }
            }
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary)
            {
                var child = ConvertScalarExpression(unary.Expression, tables, calculatedFields, param, ref constantValue);

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
                            args[i] = ConvertScalarExpression(func.Parameters[i], tables, calculatedFields, param, ref constantValue);
                        break;

                    default:
                        args = new Expression[func.Parameters.Count];

                        for (var i = 0; i < func.Parameters.Count; i++)
                            args[i] = ConvertScalarExpression(func.Parameters[i], tables, calculatedFields, param, ref constantValue);
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
                var converted = searchedCase.ElseExpression == null ? Expression.Constant(null) : ConvertScalarExpression(searchedCase.ElseExpression, tables, calculatedFields, param, ref constantValue);

                foreach (var when in searchedCase.WhenClauses.Reverse())
                {
                    converted = Expression.Condition(
                        test: HandleFilterPredicate(when.WhenExpression, tables, calculatedFields, param),
                        ifTrue: ConvertScalarExpression(when.ThenExpression, tables, calculatedFields, param, ref constantValue),
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
                var value = ConvertScalarExpression(simpleCase.InputExpression, tables, calculatedFields, param, ref constantValue);
                var convertedType = ConvertScalarExpression(simpleCase.WhenClauses[0].ThenExpression, tables, calculatedFields, param, ref constantValue).Type;

                if (convertedType.IsValueType)
                    convertedType = typeof(Nullable<>).MakeGenericType(convertedType);

                var converted = simpleCase.ElseExpression == null ? Expression.Constant(null, convertedType) : ConvertScalarExpression(simpleCase.ElseExpression, tables, calculatedFields, param, ref constantValue);

                foreach (var when in simpleCase.WhenClauses.Reverse())
                {
                    converted = Expression.Condition(
                        test: ConvertBooleanComparison(value, ConvertScalarExpression(when.WhenExpression, tables, calculatedFields, param, ref constantValue), BooleanComparisonType.Equals, when.WhenExpression),
                        ifTrue: ConvertScalarExpression(when.ThenExpression, tables, calculatedFields, param, ref constantValue),
                        ifFalse: converted);
                }

                return converted;
            }
            else if (expr is ParenthesisExpression paren)
            {
                var value = ConvertScalarExpression(paren.Expression, tables, calculatedFields, param, ref constantValue);
                return value;
            }

            throw new NotSupportedQueryFragmentException("Unsupported expression", expr);
        }

        /// <summary>
        /// Determines the type of value that is stored in an attribute
        /// </summary>
        /// <param name="attribute">The details of the attribute</param>
        /// <param name="set">Indicates if the type is to be used for insert/update operations</param>
        /// <returns>The type of values that can be stored in the attribute</returns>
        private static Type GetAttributeType(AttributeMetadata attribute, bool set)
        {
            switch (attribute.AttributeType.Value)
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
                    return set ? typeof(OptionSetValue) : typeof(int?);

                case AttributeTypeCode.String:
                    return typeof(string);

                case AttributeTypeCode.Uniqueidentifier:
                    return typeof(Guid?);

                case AttributeTypeCode.Virtual:
                    if (!String.IsNullOrEmpty(attribute.AttributeOf))
                        return typeof(string);
                    return null;
            }

            return null;
        }

        /// <summary>
        /// Gets the value from an attribute to use when evaluating expressions
        /// </summary>
        /// <param name="entity">The entity to get the value from</param>
        /// <param name="attrName">The name of the attribute to get the value from</param>
        /// <returns>The value stored in the requested attribute to use in expressions</returns>
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

        /// <summary>
        /// Gets the value stored in a literal value
        /// </summary>
        /// <param name="literal">The SQL literal value</param>
        /// <returns>The value converted to the appropriate type for use in expressions</returns>
        private object ConvertLiteralValue(Literal literal)
        {
            switch (literal.LiteralType)
            {
                case LiteralType.Integer:
                    return Int32.Parse(literal.Value, CultureInfo.InvariantCulture);

                case LiteralType.Money:
                case LiteralType.Numeric:
                    return Decimal.Parse(literal.Value, CultureInfo.InvariantCulture);

                case LiteralType.Null:
                    return null;

                case LiteralType.Real:
                    return Double.Parse(literal.Value, CultureInfo.InvariantCulture);

                case LiteralType.String:
                    return literal.Value;
            }

            throw new NotSupportedQueryFragmentException("Unsupported expression", literal);
        }

        /// <summary>
        /// Converts a SELECT query
        /// </summary>
        /// <param name="select">The SQL query to convert</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private SelectQuery ConvertSelectStatement(SelectStatement select, bool forceAggregateExpression)
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

            if (querySpec.FromClause == null)
                throw new NotSupportedQueryFragmentException("No source entity specified", querySpec);

            // Check if this query is against regular data entities or metadata.
            var queryType = new QueryTypeVisitor();
            querySpec.FromClause.Accept(queryType);

            // We can't combine different types of data - each query must have a single ultimate data source
            if (queryType.IsData && queryType.IsMetadata)
                throw new NotSupportedQueryFragmentException("Cannot combine metadata and data", querySpec.FromClause);

            if (queryType.IsGlobalOptionSet && queryType.IsEntityMetadata)
                throw new NotSupportedQueryFragmentException("Cannot combine entity metadata and global optionset metadata", querySpec.FromClause);

            if (queryType.IsMetadata && !queryType.IsGlobalOptionSet && !queryType.IsEntityMetadata)
                throw new NotSupportedQueryFragmentException("Cannot query label, localizedlabel or option entities directly. They must be combined with another metadata entity type", querySpec.FromClause);

            // Only regular data queries natively support aggregates. For any metadata queries, make sure we force
            // them to be done in memory.
            if (!queryType.IsData)
                forceAggregateExpression = true;

            if (queryType.IsData && ForceTDSEndpoint)
                return ConvertSelectStatementTDS(select, forceAggregateExpression, queryType, querySpec);

            try
            {
                return ConvertSelectStatementFetchXml(select, forceAggregateExpression, queryType, querySpec);
            }
            catch (NotSupportedQueryFragmentException)
            {
                // Check if we can still execute the raw query using the TDS endpoint instead
                if (!TDSEndpointAvailable || !queryType.IsData)
                    throw;

                return ConvertSelectStatementTDS(select, forceAggregateExpression, queryType, querySpec);
            }
        }

        private SelectQuery ConvertSelectStatementFetchXml(SelectStatement select, bool forceAggregateExpression, QueryTypeVisitor queryType, QuerySpecification querySpec)
        {
            // Store the original SQL again now so we can re-parse it if required to generate an alternative query
            // to handle aggregates via an expression rather than FetchXML.
            var sql = select.ToSql();

            // Convert the query from the "inside out":
            // 1. FROM clause gives the data source for everything else
            // 2. WHERE clause filters the initial result set
            // 3. GROUP BY changes the schema of the data source after filtering
            // 4. HAVING clause filters the result set after grouping
            // 5. SELECT clause defines the final columns to include in the results
            // 6. DISTINCT clause checks for unique-ness across the columns in the SELECT clause
            // 7. ORDER BY clause sequences the results
            // 8. OFFSET / TOP clauses skips rows in the final results

            // At each point, convert as much of the query as possible to FetchXML. Anything that can't be converted
            // directly should be handled in-memory using an expression. If any previous step has generated an expression,
            // all later steps must use expressions only as the FetchXML results will be incomplete.
            var extensions = new List<IQueryExtension>();

            if (queryType.IsMetadata)
            {
                // Only minimal FetchXML support is provided in the query implementation for metadata, so force as much as
                // possible into memory
                extensions.Add(new NoOp());
            }

            var fetch = new FetchXml.FetchType();
            var tables = HandleFromClause(querySpec.FromClause, fetch);
            HandleWhereClause(querySpec.WhereClause, tables, extensions);
            var computedColumns = HandleGroupByClause(querySpec, fetch, tables, extensions, forceAggregateExpression) ?? new Dictionary<string, Type>();
            var columns = HandleSelectClause(querySpec, fetch, tables, computedColumns, extensions);
            HandleDistinctClause(querySpec, fetch, columns, extensions);
            HandleOrderByClause(querySpec, fetch, tables, columns, computedColumns, extensions);
            HandleHavingClause(querySpec, computedColumns, extensions);
            HandleOffsetClause(querySpec, fetch, extensions);
            HandleTopClause(querySpec.TopRowFilter, fetch, extensions);

            // Sort the elements in the query so they're in the order users expect based on online samples
            foreach (var table in tables)
                table.Sort();

            // Return the final query
            SelectQuery query;

            if (queryType.IsGlobalOptionSet)
                query = new GlobalOptionSetQuery();
            else if (queryType.IsEntityMetadata)
                query = new EntityMetadataQuery();
            else
                query = new SelectQuery();

            query.FetchXml = fetch;
            query.ColumnSet = columns;
            query.AllPages = fetch.page == null && fetch.count == null;

            foreach (var extension in extensions)
                query.Extensions.Add(extension);

            if (fetch.aggregateSpecified && fetch.aggregate)
            {
                // We've generated a FetchXML aggregate query. These can generate errors when there are a lot of source records involved in the query,
                // so convert the query again, this time processing the aggregates via expressions.
                // We'll have changed the SQL DOM during the conversion to FetchXML, so recreate it by parsing the original SQL again
                var dom = new TSql150Parser(QuotedIdentifiers);
                var fragment = dom.Parse(new StringReader(sql), out _);
                var script = (TSqlScript)fragment;
                script.Accept(new ReplacePrimaryFunctionsVisitor());
                var originalSelect = (SelectStatement)script.Batches.Single().Statements.Single();

                query.AggregateAlternative = ConvertSelectStatement(originalSelect, true);
            }

            return query;
        }

        private SelectQuery ConvertSelectStatementTDS(SelectStatement select, bool forceAggregateExpression, QueryTypeVisitor queryType, QuerySpecification querySpec)
        {
            select.Accept(new AddDefaultTableAliasesVisitor());
            select.Accept(new AddDefaultColumnAliasesVisitor());
            select.ScriptTokenStream = null;

            return new SelectQuery
            {
                TSql = select.ToSql(),
                ColumnSet = querySpec.SelectElements.OfType<SelectScalarExpression>().Select(col => col.ColumnName.Value).ToArray()
            };
        }

        /// <summary>
        /// Converts the HAVING clause of a SELECT statement to a post-processing extensino
        /// </summary>
        /// <param name="querySpec">The query to convert</param>
        /// <param name="computedColumns">The grouped and aggregated columns created by a GROUP BY clause</param>
        /// <param name="extensions">The query extensions to be applied to the results of the FetchXML</param>
        private void HandleHavingClause(QuerySpecification querySpec, IDictionary<string, Type> computedColumns, List<IQueryExtension> extensions)
        {
            if (querySpec.HavingClause == null)
                return;

            // HAVING doesn't have a FetchXML equivalent, so convert directly to expression
            var expression = HandleFilterPredicate(querySpec.HavingClause.SearchCondition, new List<EntityTable>(), computedColumns, _param);
            var filter = Expression.Lambda<Func<Entity, bool>>(expression, _param).Compile();
            extensions.Add(new Having(filter));
        }

        /// <summary>
        /// Converts the GROUP BY clause of a SELECT statement to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the GROUP BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="extensions">A list of extensions to be applied to the results of the FetchXML</param>
        /// <param name="forceAggregateExpression">Indicates if aggregates should be converted to expressions even if it could be converted to FetchXML</param>
        private IDictionary<string,Type> HandleGroupByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, IList<IQueryExtension> extensions, bool forceAggregateExpression)
        {
            // Check if there is a GROUP BY clause or aggregate functions to convert
            if (querySpec.GroupByClause == null)
            {
                var aggregates = new AggregateCollectingVisitor();
                aggregates.GetAggregates(querySpec);
                if (aggregates.SelectAggregates.Count == 0 && aggregates.Aggregates.Count == 0)
                    return null;
            }
            else
            {
                if (querySpec.GroupByClause.All == true)
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY ALL clause", querySpec.GroupByClause);

                if (querySpec.GroupByClause.GroupByOption != GroupByOption.None)
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY option", querySpec.GroupByClause);
            }

            if (extensions.Count == 0 && !forceAggregateExpression)
            {
                try
                {
                    return HandleGroupByFetchXml(querySpec, fetch, tables);
                }
                catch (PostProcessingRequiredException)
                {
                    return HandleGroupByExpression(querySpec, fetch, tables, extensions);
                }
            }
            else
            {
                return HandleGroupByExpression(querySpec, fetch, tables, extensions);
            }
        }

        /// <summary>
        /// Converts a GROUP BY clause to expressions when they cannot be handled by FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the GROUP BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="extensions">A list of extensions to be applied to the results of the FetchXML</param>
        /// <returns>The names and types of the columns produced by the GROUP BY expression</returns>
        private IDictionary<string, Type> HandleGroupByExpression(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, IList<IQueryExtension> extensions)
        {
            // If we need to do any grouping/aggregation as expressions, we need to do it ALL as expressions, so don't attempt
            // any conversion of groupings/aggregations to FetchXML here. However, we can make the grouping more efficient by sorting
            // the results in FetchXml where possible, and we need to make sure that any columns referenced by the grouping/aggregation
            // expressions are included

            // Add all the columns we need. Process the values in the SELECT clause first so we can correctly handle aliases
            var columns = new ColumnCollectingVisitor();

            foreach (var col in querySpec.SelectElements)
                col.Accept(columns);

            querySpec.Accept(columns);

            foreach (var column in columns.Columns)
            {
                EntityTable table;
                string columnName;

                if (column.ColumnType == ColumnType.Wildcard)
                {
                    table = tables[0];
                    columnName = table.Metadata.PrimaryIdAttribute;
                }
                else
                {
                    GetColumnTableAlias(column, tables, out table);
                    columnName = column.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                }

                if (!table.GetItems().Any(a => a is FetchAttributeType attr && attr.name == columnName))
                {
                    table.AddItem(new FetchAttributeType
                    {
                        name = columnName
                    });
                }
            }

            // Create the grouping expressions
            var groupings = new List<Grouping>();
            var sortedGroupings = new Dictionary<FetchOrderType, List<Grouping>>();

            if (querySpec.GroupByClause != null)
            {
                foreach (var grouping in querySpec.GroupByClause.GroupingSpecifications)
                {
                    if (!(grouping is ExpressionGroupingSpecification exprGroup))
                        throw new NotSupportedQueryFragmentException("Unhandled GROUP BY expression", grouping);

                    var selector = CompileScalarExpression(exprGroup.Expression, tables, null, typeof(object), out var expression);
                    groupings.Add(new Grouping { Selector = selector, SqlExpression = exprGroup.Expression, Expression = expression });

                    if (!(exprGroup.Expression is ColumnReferenceExpression column))
                        continue;

                    // Grouping is by a single column, so add a sort for efficiency later on
                    GetColumnTableAlias(column, tables, out var table);
                    var columnName = column.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                    var sort = table.GetItems().OfType<FetchOrderType>().FirstOrDefault(order => order.attribute == columnName);
                    if (sort == null)
                    {
                        sort = new FetchOrderType
                        {
                            attribute = columnName
                        };

                        table.AddItem(sort);
                    }

                    // Keep track of which sorts are used for which groupings
                    if (!sortedGroupings.TryGetValue(sort, out var groupingsUsingSort))
                    {
                        groupingsUsingSort = new List<Grouping>();
                        sortedGroupings.Add(sort, groupingsUsingSort);
                    }

                    groupingsUsingSort.Add(groupings.Last());
                }
            }

            if (sortedGroupings.Count > 0)
            {
                // Sort the groupings according to how the sort orders will be applied
                var sorts = GetSorts(tables[0].Entity);
                var i = 0;

                foreach (var sort in sorts)
                {
                    if (!sortedGroupings.TryGetValue(sort, out var groupingsUsingSort))
                        break;

                    foreach (var group in groupingsUsingSort)
                    {
                        group.Sorted = true;
                        groupings.Remove(group);
                        groupings.Insert(i, group);
                        i++;
                    }
                }
            }

            // Create a name for the column that holds the grouping value in the reuslt set
            for (var i = 0; i < groupings.Count; i++)
                groupings[i].OutputName = $"grp{i + 1}";

            // Create the aggregate functions
            var aggregateCollector = new AggregateCollectingVisitor();
            aggregateCollector.GetAggregates(querySpec);
            var aggregates = new List<AggregateFunction>();

            foreach (var aggregate in aggregateCollector.Aggregates.Concat(aggregateCollector.SelectAggregates.Select(s => (FunctionCall) s.Expression)))
            {
                Func<Entity, object> selector = null;
                Expression expression = null;

                if (!(aggregate.Parameters[0] is ColumnReferenceExpression col) || col.ColumnType != ColumnType.Wildcard)
                    selector = CompileScalarExpression(aggregate.Parameters[0], tables, null, typeof(object), out expression);

                AggregateFunction aggregateFunction;

                switch (aggregate.FunctionName.Value.ToUpper())
                {
                    case "AVG":
                        aggregateFunction = new Average(selector);
                        break;

                    case "COUNT":
                        if (selector == null)
                            aggregateFunction = new Count(selector);
                        else if (aggregate.UniqueRowFilter == UniqueRowFilter.Distinct)
                            aggregateFunction = new CountColumnDistinct(selector);
                        else
                            aggregateFunction = new CountColumn(selector);
                        break;

                    case "MAX":
                        aggregateFunction = new Max(selector);
                        break;

                    case "MIN":
                        aggregateFunction = new Min(selector);
                        break;

                    case "SUM":
                        aggregateFunction = new Sum(selector);
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unknown aggregate function", aggregate);
                }

                aggregateFunction.SqlExpression = aggregate;
                aggregateFunction.Expression = expression;
                aggregates.Add(aggregateFunction);

                // Create a name for the column that holds the aggregate value in the result set. The name must be consistent with
                // what would be generated for the same column when the aggregate is converted directly to FetchXML.
                if (aggregate.Parameters[0] is ColumnReferenceExpression colRef)
                {
                    string attrName;
                    EntityTable table;

                    if (colRef.ColumnType == ColumnType.Wildcard)
                    {
                        attrName = tables[0].Metadata.PrimaryIdAttribute;
                        table = tables[0];
                    }
                    else
                    {
                        GetColumnTableAlias(colRef, tables, out table);
                        attrName = GetColumnAttribute(table, colRef);
                    }

                    var name = attrName;

                    if (table != tables[0])
                        name = table.Alias + "_" + name;

                    name += "_" + aggregate.FunctionName.Value.ToLower();
                    aggregateFunction.OutputName = name;
                }
                else
                {
                    aggregateFunction.OutputName = $"agg{aggregates.Count + 1}";
                }
            }

            // If there are groupings that are not already sorted in FetchXML, sort on them before aggregation as
            // the Aggregate class requires its input to be sorted
            if (groupings.Any(g => !g.Sorted))
                extensions.Add(new Sort(groupings.Select(g => new SortExpression(g.Sorted, g.Selector, false)).ToArray()));

            // Add the grouping & aggregation into the post-processing chain
            extensions.Add(new Aggregate(groupings, aggregates));

            // Rewrite the rest of the query to refer to the new names for the grouped & aggregated data
            var rewrites = new Dictionary<ScalarExpression, string>();

            foreach (var grouping in groupings)
                rewrites[grouping.SqlExpression] = grouping.OutputName;

            foreach (var aggregate in aggregates)
                rewrites[aggregate.SqlExpression] = aggregate.OutputName;

            var rewrite = new RewriteVisitor(rewrites);
            querySpec.Accept(rewrite);

            var outputColumns = new Dictionary<string, Type>();

            foreach (var grouping in groupings)
                outputColumns[grouping.OutputName] = grouping.Expression.Type;

            foreach (var aggregate in aggregates)
                outputColumns[aggregate.OutputName] = aggregate.Type;

            return outputColumns;
        }

        /// <summary>
        /// Gets the sorts from a query in the order in which they will be applied
        /// </summary>
        /// <param name="root">The entity or link-entity object to get the sorts from</param>
        /// <returns>The sequence of sorts that are applied by the query</returns>
        private IEnumerable<FetchOrderType> GetSorts(object root)
        {
            object[] items = null;

            if (root is FetchEntityType entity)
                items = entity.Items;
            else if (root is FetchLinkEntityType link)
                items = link.Items;

            if (items == null)
                yield break;

            foreach (var sort in items.OfType<FetchOrderType>())
                yield return sort;

            foreach (var child in items.OfType<FetchLinkEntityType>())
            {
                foreach (var childSort in GetSorts(child))
                    yield return childSort;
            }
        }

        /// <summary>
        /// Converts a GROUP BY clause to expressions when they cannot be handled by FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the GROUP BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="extensions">A list of extensions to be applied to the results of the FetchXML</param>
        /// <returns>The names and types of the columns produced by the GROUP BY expression</returns>
        private IDictionary<string,Type> HandleGroupByFetchXml(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            // Check if all groupings and aggregate functions are supported in FetchXML
            var groupValidationVisitor = new GroupValidationVisitor();
            querySpec.Accept(groupValidationVisitor);

            if (!groupValidationVisitor.Valid)
                throw new PostProcessingRequiredException("Unhandled GROUP BY/aggregate expression", groupValidationVisitor.InvalidFragment);

            var outputColumns = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
            var rewrites = new Dictionary<ScalarExpression, string>();

            // Set the aggregate flag on the FetchXML query
            fetch.aggregate = true;
            fetch.aggregateSpecified = true;

            // Process each field that the data should be grouped by
            var uniqueGroupings = new HashSet<string>();

            foreach (var exprGroup in querySpec.GroupByClause?.GroupingSpecifications?.Cast<ExpressionGroupingSpecification>() ?? Array.Empty<ExpressionGroupingSpecification>())
            {
                var expr = exprGroup.Expression;
                DateGroupingType? dateGrouping = null;

                if (expr is FunctionCall func)
                {
                    if (!TryParseDatePart(func, out var g, out var dateCol))
                        throw new PostProcessingRequiredException("Unhandled GROUP BY clause - column name or DATEPART function expected", expr);

                    dateGrouping = g;
                    expr = dateCol;
                }

                if (!(expr is ColumnReferenceExpression col))
                    throw new PostProcessingRequiredException("Unhandled GROUP BY clause - column name or DATEPART function expected", expr);

                // Find the table in the query that the grouping attribute is from
                GetColumnTableAlias(col, tables, out var table);
                var columnName = GetColumnAttribute(table, col);

                if (table == null)
                    throw new NotSupportedQueryFragmentException("Unknown table", col);

                // Ignore any cases where we are grouping by the same column twice
                var groupingKey = table.Alias + "." + columnName + "." + dateGrouping;
                if (!uniqueGroupings.Add(groupingKey))
                    continue;

                var metadata = table.Metadata.Attributes.Single(a => a.LogicalName == columnName);

                if (!String.IsNullOrEmpty(metadata.AttributeOf))
                    throw new PostProcessingRequiredException("Cannot group by virtual attribute");

                var type = GetAttributeType(metadata, false);

                // If the attribute isn't already included, add it to the appropriate table
                var attr = new FetchAttributeType
                {
                    name = columnName,
                    groupby = FetchBoolType.@true,
                    groupbySpecified = true
                };

                if (dateGrouping != null)
                {
                    attr.dategrouping = dateGrouping.Value;
                    attr.dategroupingSpecified = true;
                }

                table.AddItem(attr);

                // Find the references to this grouping in the SELECT clause so we include it with all those aliases
                var selectElements = querySpec.SelectElements.OfType<SelectScalarExpression>().Where(s =>
                    {
                        var selectExpr = s.Expression;

                        if (selectExpr is FunctionCall selectFunc && TryParseDatePart(selectFunc, out var selectDateGrouping, out var selectDateCol))
                        {
                            selectExpr = selectDateCol;

                            if (selectDateGrouping != dateGrouping)
                                return false;
                        }

                        if (!(s.Expression is ColumnReferenceExpression selectCol))
                            return false;

                        GetColumnTableAlias(selectCol, tables, out var selectTable);

                        if (GetColumnAttribute(selectTable, selectCol) != attr.name)
                            return false;

                        if (selectTable != table)
                            return false;

                        return true;
                    })
                    .ToList();

                Func<string> generateAlias = () =>
                {
                    var alias = attr.name;
                    var num = 0;

                    while (tables.SelectMany(t => t.GetItems()).OfType<FetchAttributeType>().Any(a => a.alias == alias))
                        alias = attr.name + (++num);

                    return alias;
                };

                if (selectElements.Count > 0)
                {
                    // Use the existing <attribute> generated for the GROUP BY clause for the first instance of the column in the SELECT clause
                    attr.alias = selectElements[0].ColumnName?.Identifier?.Value ?? generateAlias();
                    outputColumns[attr.alias] = type;

                    foreach (var duplicateSelectElement in selectElements.Skip(1))
                    {
                        // Create additional <attribute> elements for any later instances of the same attribute in the SELECT clause
                        var duplicateAttr = new FetchAttributeType
                        {
                            name = attr.name,
                            groupby = FetchBoolType.@true,
                            groupbySpecified = true,
                            dategrouping = attr.dategrouping,
                            dategroupingSpecified = attr.dategroupingSpecified,
                            alias = duplicateSelectElement.ColumnName?.Identifier?.Value ?? generateAlias()
                        };

                        table.AddItem(duplicateAttr);
                        outputColumns[duplicateAttr.alias] = type;
                    }
                }
                else
                {
                    var alias = generateAlias();
                    attr.alias = alias;
                    outputColumns[alias] = type;
                }

                rewrites[exprGroup.Expression] = attr.alias;
            }

            // Create the aggregate functions
            var aggregateCollector = new AggregateCollectingVisitor();
            aggregateCollector.GetAggregates(querySpec);

            foreach (var func in aggregateCollector.Aggregates.Select(f => new { Alias = (string)null, Func = f })
                .Concat(aggregateCollector.SelectAggregates.Select(s => new { Alias = s.ColumnName?.Identifier?.Value, Func = (FunctionCall)s.Expression })))
            {
                var col = (ColumnReferenceExpression)func.Func.Parameters[0];
                string attrName;
                EntityTable table;

                if (col.ColumnType == ColumnType.Wildcard)
                {
                    attrName = tables[0].Metadata.PrimaryIdAttribute;
                    table = tables[0];
                }
                else
                {
                    GetColumnTableAlias(col, tables, out table);
                    attrName = GetColumnAttribute(table, col);
                }

                var attr = new FetchAttributeType { name = attrName };
                table.AddItem(attr);

                switch (func.Func.FunctionName.Value.ToLower())
                {
                    case "count":
                        // Select the appropriate aggregate depending on whether we're doing count(*) or count(field)
                        attr.aggregate = col.ColumnType == ColumnType.Wildcard ? AggregateType.count : AggregateType.countcolumn;
                        attr.aggregateSpecified = true;

                        // If the column is the primary ID column of the main table we can use count instead of countcolumn
                        // and therefore use RetrieveTotalRecordCount message
                        if (table == tables[0] && attr.aggregate == AggregateType.countcolumn && attrName == table.Metadata.PrimaryIdAttribute)
                            attr.aggregate = AggregateType.count;
                        break;

                    case "avg":
                    case "min":
                    case "max":
                    case "sum":
                        // All other aggregates can be applied directly
                        attr.aggregate = (AggregateType)Enum.Parse(typeof(AggregateType), func.Func.FunctionName.Value.ToLower());
                        attr.aggregateSpecified = true;
                        break;

                    default:
                        // No other function calls are supported
                        throw new NotSupportedQueryFragmentException("Unhandled function", func.Func);
                }

                if (func.Func.UniqueRowFilter == UniqueRowFilter.Distinct)
                {
                    // Handle `count(distinct col)` expressions
                    attr.distinct = FetchBoolType.@true;
                    attr.distinctSpecified = true;
                }

                // Create a unique alias for each aggregate function
                // Check if there is an alias for this function in the SELECT clause
                if (func.Alias != null)
                {
                    attr.alias = func.Alias;
                }
                else
                {
                    var baseAlias = attr.name + "_" + func.Func.FunctionName.Value.ToLower();
                    if (table != tables[0])
                        baseAlias = table.Alias + "_" + baseAlias;

                    var alias = baseAlias;
                    var num = 1;

                    while (tables.SelectMany(t => t.GetItems()).OfType<FetchAttributeType>().Any(a => a.alias == alias))
                        alias = baseAlias + "_" + (++num);

                    attr.alias = alias;
                }

                rewrites[func.Func] = attr.alias;

                switch (attr.aggregate)
                {
                    case AggregateType.count:
                    case AggregateType.countcolumn:
                        outputColumns[attr.alias] = typeof(int);
                        break;

                    case AggregateType.avg:
                        outputColumns[attr.alias] = typeof(decimal);
                        break;

                    default:
                        var metadata = table.Metadata.Attributes.Single(a => a.LogicalName == attr.name);
                        outputColumns[attr.alias] = GetAttributeType(metadata, false);
                        break;
                }
            }

            // Rewrite the rest of the query to refer to the new names for the grouped & aggregated data
            var rewrite = new RewriteVisitor(rewrites);
            querySpec.Accept(rewrite);

            return outputColumns;
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
        /// <param name="columns">The columns that will be displayed in the results</param>
        /// <param name="extensions">Any extension methods to be applied to the results of the query</param>
        private void HandleDistinctClause(QuerySpecification querySpec, FetchXml.FetchType fetch, string[] columns, IList<IQueryExtension> extensions)
        {
            if (querySpec.UniqueRowFilter == UniqueRowFilter.Distinct)
            {
                if (extensions.Count == 0)
                {
                    fetch.distinct = true;
                    fetch.distinctSpecified = true;
                }
                else
                {
                    extensions.Add(new Distinct(columns));
                }
            }
        }

        /// <summary>
        /// Converts the OFFSET clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the OFFSET clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        private void HandleOffsetClause(QuerySpecification querySpec, FetchXml.FetchType fetch, IList<IQueryExtension> extensions)
        {
            // The OFFSET clause doesn't have a direct equivalent in FetchXML, but in some circumstances we can get the same effect
            // by going direct to a specific page. For this to work the offset must be an exact multiple of the fetch count
            if (querySpec.OffsetClause == null)
                return;

            if (!(querySpec.OffsetClause.OffsetExpression is IntegerLiteral offset))
                throw new NotSupportedQueryFragmentException("Unhandled OFFSET clause offset expression", querySpec.OffsetClause.OffsetExpression);

            if (!(querySpec.OffsetClause.FetchExpression is IntegerLiteral fetchCount))
                throw new NotSupportedQueryFragmentException("Unhandled OFFSET clause fetch expression", querySpec.OffsetClause.FetchExpression);

            var pageSize = Int32.Parse(fetchCount.Value, CultureInfo.InvariantCulture);
            var pageNumber = (decimal)Int32.Parse(offset.Value, CultureInfo.InvariantCulture) / pageSize + 1;

            if (extensions.Count == 0 && pageNumber == (int)pageNumber)
            {
                fetch.count = pageSize.ToString(CultureInfo.InvariantCulture);
                fetch.page = pageNumber.ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                extensions.Add(new Offset(Int32.Parse(offset.Value, CultureInfo.InvariantCulture), pageSize));
            }
        }

        /// <summary>
        /// Converts the TOP clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="top">The TOP clause of the SELECT query to convert from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        private void HandleTopClause(TopRowFilter top, FetchXml.FetchType fetch, IList<IQueryExtension> extensions)
        {
            if (top == null)
                return;

            if (top.Percent)
                throw new NotSupportedQueryFragmentException("Unhandled TOP PERCENT clause", top);

            if (top.WithTies)
                throw new NotSupportedQueryFragmentException("Unhandled TOP WITH TIES clause", top);

            var expr = top.Expression;

            while (expr is ParenthesisExpression paren)
                expr = paren.Expression;

            if (!(expr is IntegerLiteral topLiteral))
                throw new NotSupportedQueryFragmentException("Unhandled TOP expression", top.Expression);

            var topCount = Int32.Parse(topLiteral.Value, CultureInfo.InvariantCulture);

            if (topCount < 1)
                throw new NotSupportedQueryFragmentException("TOP must be at least 1", topLiteral);

            if (extensions.Count == 0 && topCount <= 5000)
                fetch.top = topLiteral.Value;
            else
                extensions.Add(new Top(topCount));
        }

        /// <summary>
        /// Converts the ORDER BY clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the ORDER BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        /// <returns>The sorts to apply to the results of the query</returns>
        private void HandleOrderByClause(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Type> calculatedFields, IList<IQueryExtension> extensions)
        {
            if (querySpec.OrderByClause == null)
                return;

            // If there's any sorts from previous extensions, do all these sorts in memory
            var useFetchSorts = !tables.SelectMany(t => t.GetItems()).OfType<FetchOrderType>().Any();

            if (useFetchSorts)
            {
                try
                {
                    HandleOrderByClauseFetchXml(querySpec, fetch, tables, columns, calculatedFields);
                }
                catch (PostProcessingRequiredException)
                {
                    var sorts = HandleOrderByClauseExpression(querySpec, fetch, tables, columns, calculatedFields, true);
                    extensions.Add(new Sort(sorts));
                }
            }
            else
            {
                var sorts = HandleOrderByClauseExpression(querySpec, fetch, tables, columns, calculatedFields, false);
                extensions.Add(new Sort(sorts));
            }
        }

        /// <summary>
        /// Converts the ORDER BY clause of a SELECT query to FetchXML
        /// </summary>
        /// <param name="querySpec">The SELECT query to convert the ORDER BY clause from</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        private void HandleOrderByClauseFetchXml(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Type> calculatedFields)
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
                        var colName = columns[Int32.Parse(colIndex.Value, CultureInfo.InvariantCulture) - 1];

                        col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

                        foreach (var part in colName.Split('.'))
                            col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });

                        if (col.MultiPartIdentifier.Identifiers.Count == 1 && tables.Count > 1)
                            col.MultiPartIdentifier.Identifiers.Insert(0, new Identifier { Value = tables[0].Alias ?? tables[0].EntityName });
                    }
                    else if (fetch.aggregateSpecified && fetch.aggregate && sort.Expression is FunctionCall)
                    {
                        // Check if we already have this aggregate as an attribute. If not, add it.
                        var alias = "";
                        AddAttribute(tables, sort.Expression, calculatedFields, out var table, out var attr, ref alias);

                        // Finally, sort by the alias of the aggregate attribute
                        col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };
                        col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = alias });
                    }
                    else
                    {
                        throw new PostProcessingRequiredException("Unsupported ORDER BY clause", sort.Expression);
                    }
                }

                // Find the table from which the column is taken
                GetColumnTableAlias(col, tables, out var orderTable, calculatedFields);
                var attrName = GetColumnAttribute(orderTable, col);

                if (!orderTable.GetItems().OfType<FetchAttributeType>().Any(a => a.alias?.Equals(attrName, StringComparison.OrdinalIgnoreCase) == true) &&
                    col.MultiPartIdentifier.Identifiers.Count == 1 && 
                    calculatedFields != null && 
                    calculatedFields.ContainsKey(col.MultiPartIdentifier.Identifiers[0].Value))
                    throw new PostProcessingRequiredException("Cannot sort by calculated field", sort.Expression);

                // Can't control sequence of orders between link-entities. Orders are always applied in depth-first-search order, so
                // check there is no order already applied on a later entity.
                if (LaterEntityHasOrder(tables, orderTable))
                    throw new PostProcessingRequiredException("Order already applied to later link-entity", sort.Expression);

                var attrMetadata = orderTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                if (!String.IsNullOrEmpty(attrMetadata?.AttributeOf))
                {
                    if (attrName == attrMetadata.AttributeOf + "name")
                        attrName = attrMetadata.AttributeOf;
                    else
                        throw new PostProcessingRequiredException("Cannot sort by virtual attribute");
                }
                
                var order = new FetchOrderType
                {
                    attribute = attrName,
                    descending = sort.SortOrder == SortOrder.Descending
                };

                // For aggregate queries, ordering must be done on aliases not attributes
                if (fetch.aggregate)
                {
                    var attr = orderTable.GetItems()
                        .OfType<FetchAttributeType>()
                        .SingleOrDefault(a => a.alias == order.attribute);

                    if (attr == null)
                    {
                        attr = orderTable.GetItems()
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
                    var containsAliasedAttr = orderTable.GetItems().Any(i => i is FetchAttributeType a && a.name.Equals(order.attribute) && a.alias != null && a.alias != a.name);

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
        private SortExpression[] HandleOrderByClauseExpression(QuerySpecification querySpec, FetchXml.FetchType fetch, List<EntityTable> tables, string[] columns, IDictionary<string, Type> calculatedFields, bool useFetchSorts)
        {
            // Check how many sorts were already converted to native FetchXML - we can use these results to only sort partial sequences
            // of results rather than having to sort the entire result set in memory.
            var fetchXmlSorts = useFetchSorts ? tables.SelectMany(t => t.GetItems().OfType<FetchOrderType>()).Count() : 0;

            // Convert each ORDER BY expression in turn
            var sortNumber = 0;
            var sorts = new List<SortExpression>();

            foreach (var sort in querySpec.OrderByClause.OrderByElements)
            {
                var expression = sort.Expression;

                if (expression is IntegerLiteral colIndex)
                {
                    var colName = columns[Int32.Parse(colIndex.Value, CultureInfo.InvariantCulture) - 1];

                    expression = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

                    foreach (var part in colName.Split('.'))
                        ((ColumnReferenceExpression)expression).MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });
                }

                sortNumber++;
                var isFetchXml = sortNumber <= fetchXmlSorts;
                var selector = CompileScalarExpression(expression, tables, calculatedFields, typeof(object), out _);
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
            var items = entityTable.GetItems();

            if (items.Length == 0)
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
        /// <param name="extensions">A list of extensions to be applied to the results of the FetchXML</param>
        private void HandleWhereClause(WhereClause where, List<EntityTable> tables, IList<IQueryExtension> extensions)
        {
            // Check if there is a WHERE clause to apply
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
            HandleFilter(where.SearchCondition, filter, tables, tables[0], null, true, ref col1, ref col2, ref postFilterExpression, param);

            // If no specific logical operator was found, switch to "and"
            if (filter.type == (filterType)2)
                filter.type = filterType.and;

            if (postFilterExpression != null)
            {
                var postFilter = Expression.Lambda<Func<Entity, bool>>(postFilterExpression, param).Compile();
                extensions.Add(new Where(postFilter));
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
        /// <param name="col1">Identifies the first column in the filter (for JOIN purposes)</param>
        /// <param name="col2">Identifies the second column in the filter (for JOIN purposes)</param>
        /// <param name="postFilter">The additional in-memory predicate to be applied to any results</param>
        /// <param name="param">The entity parameter to be supplied to the <paramref name="postFilter"/></param>
        private void HandleFilter(BooleanExpression searchCondition, filter criteria, List<EntityTable> tables, EntityTable targetTable, IDictionary<string,Type> computedColumns, bool where, ref ColumnReferenceExpression col1, ref ColumnReferenceExpression col2, ref Expression postFilter, ParameterExpression param)
        {
            try
            {
                HandleFilterFetchXml(searchCondition, criteria, tables, targetTable, computedColumns, where, false, ref col1, ref col2, ref postFilter, param);
            }
            catch (PostProcessingRequiredException)
            {
                if (!where)
                    throw;

                var predicate = HandleFilterPredicate(searchCondition, tables, computedColumns, param);

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
        /// <param name="postFilter">The additional in-memory predicate to be applied to any results</param>
        /// <param name="param">The entity parameter to be supplied to the <paramref name="postFilter"/></param>
        private void HandleFilterFetchXml(BooleanExpression searchCondition, filter criteria, List<EntityTable> tables, EntityTable targetTable, IDictionary<string,Type> computedColumns, bool where, bool inOr, ref ColumnReferenceExpression col1, ref ColumnReferenceExpression col2, ref Expression postFilter, ParameterExpression param)
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
                var expr = comparison.SecondExpression;
                var type = comparison.ComparisonType;

                if (field != null && field2 != null)
                {
                    // The operator is comparing two attributes. This is allowed in join criteria,
                    // but not in filter conditions before version 9.1.0.19251
                    if (where)
                    {
                        if (!ColumnComparisonAvailable)
                            throw new PostProcessingRequiredException("Unsupported comparison", comparison);
                    }
                    else
                    {
                        if (col1 == null && col2 == null && type == BooleanComparisonType.Equals)
                        {
                            // We've found the join columns - don't apply this as an extra filter
                            if (inOr)
                                throw new NotSupportedQueryFragmentException("Cannot combine join criteria with OR", comparison);

                            col1 = field;
                            col2 = field2;
                            return;
                        }

                        if (!ColumnComparisonAvailable)
                            throw new NotSupportedQueryFragmentException("Unsupported comparison - rewrite as WHERE clause", comparison);
                    }
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
                if (field == null || (literal == null && func == null && (field2 == null || !ColumnComparisonAvailable) && !IsConstantValueExpression(expr, tables, null, _param, out literal)))
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
                    if (IsConstantValueExpression(func, tables, null, _param, out literal))
                        value = literal.Value;
                    else
                        throw new PostProcessingRequiredException("Unsupported FetchXML function", func);
                }

                // Find the entity that the condition applies to, which may be different to the entity that the condition FetchXML element will be 
                // added within
                var entityName = GetColumnTableAlias(field, tables, out var entityTable);

                if (entityTable == targetTable)
                    entityName = null;

                if (field2 == null)
                {
                    var attrName = GetColumnAttribute(entityTable, field);
                    var attribute = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                    if (!String.IsNullOrEmpty(attribute?.AttributeOf) && entityTable.Metadata.Attributes.Any(a => a.LogicalName == attribute.AttributeOf))
                    {
                        var baseAttribute = entityTable.Metadata.Attributes.Single(a => a.LogicalName == attribute.AttributeOf);
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

                    var condition = new condition
                    {
                        entityname = entityName,
                        attribute = attrName,
                        @operator = op,
                        value = value
                    };

                    criteria.Items = AddItem(criteria.Items, condition);

                    if (values != null)
                        condition.Items = values.Select(v => new conditionValue { Value = v }).ToArray();
                }
                else
                {
                    // Column comparisons can only happen within a single entity
                    var entityName2 = GetColumnTableAlias(field2, tables, out var entityTable2);

                    if (entityTable != entityTable2)
                        throw new PostProcessingRequiredException("Unsupported comparison", comparison);

                    var attr1 = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(GetColumnAttribute(entityTable, field), StringComparison.OrdinalIgnoreCase));
                    var attr2 = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(GetColumnAttribute(entityTable, field2), StringComparison.OrdinalIgnoreCase));

                    if (!String.IsNullOrEmpty(attr1?.AttributeOf))
                        throw new PostProcessingRequiredException("Cannot filter on virtual attributes", field);

                    if (!String.IsNullOrEmpty(attr2?.AttributeOf))
                        throw new PostProcessingRequiredException("Cannot filter on virtual attributes", field2);

                    if (entityTable2 == targetTable)
                        entityName2 = null;

                    criteria.Items = AddItem(criteria.Items, new condition
                    {
                        entityname = entityName,
                        attribute = GetColumnAttribute(entityTable2, field),
                        @operator = op,
                        valueof = GetColumnAttribute(entityTable2, field2)
                    });
                }
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
                    HandleFilterFetchXml(binary.FirstExpression, criteria, tables, targetTable, computedColumns, where, inOr || op == filterType.or, ref col1, ref col2, ref postFilter, param);
                }
                catch (PostProcessingRequiredException)
                {
                    if (inOr || op != filterType.and || !where)
                        throw;

                    var lhsPredicate = HandleFilterPredicate(binary.FirstExpression, tables, computedColumns, param);
                    if (postFilter == null)
                        postFilter = lhsPredicate;
                    else
                        postFilter = Expression.And(postFilter, lhsPredicate);
                }

                try
                {
                    HandleFilterFetchXml(binary.SecondExpression, criteria, tables, targetTable, computedColumns, where, inOr || op == filterType.or, ref col1, ref col2, ref postFilter, param);
                }
                catch (PostProcessingRequiredException)
                {
                    if (inOr || op != filterType.and || !where)
                        throw;

                    var rhsPredicate = HandleFilterPredicate(binary.SecondExpression, tables, computedColumns, param);
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

                HandleFilterFetchXml(paren.Expression, subFilter, tables, targetTable, computedColumns, where, inOr, ref col1, ref col2, ref postFilter, param);

                if (subFilter.type == (filterType)2)
                    subFilter.type = filterType.and;

                criteria.Items = AddItem(criteria.Items, subFilter);
                criteria = subFilter;
            }
            else if (searchCondition is BooleanIsNullExpression isNull)
            {
                // Handle IS NULL and IS NOT NULL expresisons
                if (!(isNull.Expression is ColumnReferenceExpression field))
                    throw new NotSupportedQueryFragmentException("Unsupported comparison", isNull.Expression);

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                var attrName = GetColumnAttribute(entityTable, field);
                var attr = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                if (!String.IsNullOrEmpty(attr.AttributeOf) && entityTable.Metadata.Attributes.Any(a => a.LogicalName == attr.AttributeOf))
                {
                    var virtualAttributeHandled = false;
                    var baseAttribute = entityTable.Metadata.Attributes.Single(a => a.LogicalName == attr.AttributeOf);

                    // If filtering on the name of an optionset value, filter on the underlying value instead as a null name implies a null
                    // value and a non-null name implies a non-null value
                    if (attr.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is EnumAttributeMetadata)
                    {
                        attrName = baseAttribute.LogicalName;
                        virtualAttributeHandled = true;
                    }

                    // If filtering on the name of a lookup value, add a join to the target type and filter on the primary name
                    // attribute instead. It's possible to have a non-null lookup value that refers to a null name.
                    if (attr.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is LookupAttributeMetadata lookupAttr && lookupAttr.Targets.Length == 1)
                    {
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
                    }

                    if (!virtualAttributeHandled)
                        throw new PostProcessingRequiredException("Cannot filter on virtual attribute", field);
                }

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = entityName,
                    attribute = attrName,
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

                var attrName = GetColumnAttribute(entityTable, field);
                var attr = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                if (!String.IsNullOrEmpty(attr.AttributeOf) && entityTable.Metadata.Attributes.Any(a => a.LogicalName == attr.AttributeOf))
                {
                    var virtualAttributeHandled = false;
                    var baseAttribute = entityTable.Metadata.Attributes.Single(a => a.LogicalName == attr.AttributeOf);

                    // If filtering on a lookup attribute, add a join to the target entity and filter on the primary name
                    // attribute instead.
                    if (attr.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is LookupAttributeMetadata lookupAttr && lookupAttr.Targets.Length == 1)
                    {
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
                    }

                    if (!virtualAttributeHandled)
                        throw new PostProcessingRequiredException("Cannot filter on virtual attribute", field);
                }

                criteria.Items = AddItem(criteria.Items, new condition
                {
                    entityname = entityName,
                    attribute = attrName,
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

                var items = @in.Values
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

                var attrName = GetColumnAttribute(entityTable, field);
                var attr = entityTable.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                if (!String.IsNullOrEmpty(attr.AttributeOf) && entityTable.Metadata.Attributes.Any(a => a.LogicalName == attr.AttributeOf))
                {
                    var virtualAttributeHandled = false;
                    var baseAttribute = entityTable.Metadata.Attributes.Single(a => a.LogicalName == attr.AttributeOf);

                    // If filtering on the name of an optionset attribute, convert the names to values and filter on the underlying
                    // value attribute instead. It's possible to have multiple values with the same name.
                    if (attr.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is EnumAttributeMetadata enumAttribute)
                    {
                        attrName = baseAttribute.LogicalName;

                        items = @in.Values
                            .Cast<Literal>()
                            .SelectMany(v =>
                            {
                                var options = enumAttribute.OptionSet.Options.Where(o => o.Label.UserLocalizedLabel.Label.Equals(v.Value, StringComparison.OrdinalIgnoreCase)).ToList();

                                if (options.Count == 0)
                                    throw new NotSupportedQueryFragmentException("Unknown optionset value. Supported values are " + String.Join(", ", enumAttribute.OptionSet.Options.Select(o => o.Label.UserLocalizedLabel.Label)), v);

                                return options.Select(o => new conditionValue { Value = o.Value.ToString() });
                            })
                            .ToArray();

                        virtualAttributeHandled = true;
                    }

                    // If filtering on the name of a lookup attribute, add a join to the target entity and filter on the primary
                    // name attribute instead.
                    if (attr.LogicalName == baseAttribute.LogicalName + "name" && baseAttribute is LookupAttributeMetadata lookupAttr && lookupAttr.Targets.Length == 1)
                    {
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
                    }

                    if (!virtualAttributeHandled)
                        throw new PostProcessingRequiredException("Cannot filter on virtual attribute", field);
                }

                var condition = new condition
                {
                    entityname = entityName,
                    attribute = attrName,
                    @operator = @in.NotDefined ? @operator.notin : @operator.@in,
                    Items = items
                };

                criteria.Items = AddItem(criteria.Items, condition);
            }
            else if (searchCondition is FullTextPredicate ftp && ftp.FullTextFunctionType == FullTextFunctionType.Contains)
            {
                if (ftp.Columns.Count != 1)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches on a single column are supported", ftp);

                var field = ftp.Columns[0];

                var literal = ftp.Value as StringLiteral;

                if (literal == null)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only string literal values are supported", literal);

                var values = literal.Value.Split(new[] { " OR " }, StringSplitOptions.None);

                for (var i = 0; i < values.Length; i++)
                {
                    if (!Int32.TryParse(values[i], out _))
                        throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches in the format '1 OR 2 OR 3' are supported", literal);
                }

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                var condition = new condition
                {
                    entityname = entityName,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = @operator.containvalues
                };

                condition.Items = values.Select(v => new conditionValue { Value = v })
                    .ToArray();

                criteria.Items = AddItem(criteria.Items, condition);
            }
            else if (searchCondition is BooleanNotExpression not && not.Expression is FullTextPredicate notFtp && notFtp.FullTextFunctionType == FullTextFunctionType.Contains)
            {
                if (notFtp.Columns.Count != 1)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches on a single column are supported", notFtp);

                var field = notFtp.Columns[0];

                var literal = notFtp.Value as StringLiteral;

                if (literal == null)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only string literal values are supported", literal);

                var values = literal.Value.Split(new[] { " OR " }, StringSplitOptions.None);

                for (var i = 0; i < values.Length; i++)
                {
                    if (!Int32.TryParse(values[i], out _))
                        throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches in the format '1 OR 2 OR 3' are supported", literal);
                }

                var entityName = GetColumnTableAlias(field, tables, out var entityTable);
                if (entityTable == targetTable)
                    entityName = null;

                var condition = new condition
                {
                    entityname = entityName,
                    attribute = field.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = @operator.notcontainvalues
                };

                condition.Items = values.Select(v => new conditionValue { Value = v })
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
        /// <param name="computedColumns">The columns created by a GROUP BY clause</param>
        /// <param name="param">The parameter that identifies the entity to apply the predicate to</param>
        /// <returns>The expression that implements the <paramref name="searchCondition"/></returns>
        private Expression HandleFilterPredicate(BooleanExpression searchCondition, List<EntityTable> tables, IDictionary<string, Type> computedColumns, ParameterExpression param)
        {
            if (searchCondition is BooleanComparisonExpression comparison)
            {
                var lhs = ConvertScalarExpression(comparison.FirstExpression, tables, computedColumns, param);
                var rhs = ConvertScalarExpression(comparison.SecondExpression, tables, computedColumns, param);

                return ConvertBooleanComparison(lhs, rhs, comparison.ComparisonType, comparison);
            }
            else if (searchCondition is BooleanBinaryExpression binary)
            {
                var lhs = HandleFilterPredicate(binary.FirstExpression, tables, computedColumns, param);
                var rhs = HandleFilterPredicate(binary.SecondExpression, tables, computedColumns, param);

                if (binary.BinaryExpressionType == BooleanBinaryExpressionType.And)
                    return Expression.And(lhs, rhs);
                else
                    return Expression.Or(lhs, rhs);
            }
            else if (searchCondition is BooleanParenthesisExpression paren)
            {
                var child = HandleFilterPredicate(paren.Expression, tables, computedColumns, param);
                return child;
            }
            else if (searchCondition is BooleanIsNullExpression isNull)
            {
                var value = ConvertScalarExpression(isNull.Expression, tables, computedColumns, param);

                if (isNull.IsNot)
                    return Expression.NotEqual(value, Expression.Constant(null));
                else
                    return Expression.Equal(value, Expression.Constant(null));
            }
            else if (searchCondition is LikePredicate like)
            {
                var lhs = ConvertScalarExpression(like.FirstExpression, tables, computedColumns, param);
                var rhs = ConvertScalarExpression(like.SecondExpression, tables, computedColumns, param);
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
                var value = ConvertScalarExpression(@in.Expression, tables, computedColumns, param);

                foreach (var v in @in.Values)
                {
                    var equal = Expression.Equal(value, ConvertScalarExpression(v, tables, computedColumns, param));

                    if (converted == null)
                        converted = equal;
                    else
                        converted = Expression.Or(converted, equal);
                }

                return converted;
            }
            else if (searchCondition is FullTextPredicate ftp && ftp.FullTextFunctionType == FullTextFunctionType.Contains)
            {
                if (ftp.Columns.Count != 1)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches on a single column are supported", ftp);

                var column = ConvertScalarExpression(ftp.Columns[0], tables, computedColumns, param);

                if (column.Type != typeof(OptionSetValueCollection))
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, column must be a multi-select picklist column", ftp.Columns[0]);

                var literal = ftp.Value as StringLiteral;

                if (literal != null)
                    throw new NotSupportedQueryFragmentException("Unsupported full text search, only string literal values are supported", literal);

                var values = literal.Value.Split(new[] { " OR " }, StringSplitOptions.None);

                for (var i = 0; i < values.Length; i++)
                {
                    if (!Int32.TryParse(values[i], out _))
                        throw new NotSupportedQueryFragmentException("Unsupported full text search, only searches in the format '1 OR 2 OR 3' are supported", literal);
                }

                var ints = values.Select(v => Int32.Parse(v)).ToArray();

                var func = Expr.Call(() => ExpressionFunctions.Contains(Expr.Arg<OptionSetValueCollection>(), Expr.Arg<int[]>()),
                    column,
                    Expression.Constant(ints, typeof(int[])));

                return func;
            }
            else if (searchCondition is BooleanNotExpression not)
            {
                var inner = HandleFilterPredicate(not.Expression, tables, computedColumns, param);
                return Expression.Not(inner);
            }
            else
            {
                throw new PostProcessingRequiredException("Unhandled WHERE clause", searchCondition);
            }
        }

        private Expression ConvertBooleanComparison(Expression lhs, Expression rhs, BooleanComparisonType comparisonType, TSqlFragment fragment)
        {
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

            // If one value is a decimal and another isn't, convert them
            if (lhsValue.Type == typeof(decimal) && (rhsValue.Type == typeof(int) || rhsValue.Type == typeof(double)))
                rhsValue = Expression.Convert(rhsValue, typeof(decimal));

            if (rhsValue.Type == typeof(decimal) && (lhsValue.Type == typeof(int) || lhsValue.Type == typeof(double)))
                lhsValue = Expression.Convert(lhsValue, typeof(decimal));

            Expression coreComparison;

            switch (comparisonType)
            {
                case BooleanComparisonType.Equals:
                    if (lhsValue.Type == typeof(string) && rhsValue.Type == typeof(string))
                        coreComparison = Expression.Equal(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.CaseInsensitiveEquals(Expr.Arg<string>(), Expr.Arg<string>())));
                    else if (lhsValue.Type == typeof(EntityReference) && rhsValue.Type == typeof(Guid))
                        coreComparison = Expression.Equal(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.Equal(Expr.Arg<EntityReference>(), Expr.Arg<Guid>())));
                    else if (lhsValue.Type == typeof(Guid) && rhsValue.Type == typeof(EntityReference))
                        coreComparison = Expression.Equal(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.Equal(Expr.Arg<Guid>(), Expr.Arg<EntityReference>())));
                    else if (lhsValue.Type == typeof(EntityReference) && rhsValue.Type == typeof(EntityReference))
                        coreComparison = Expression.Equal(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.Equal(Expr.Arg<EntityReference>(), Expr.Arg<EntityReference>())));
                    else
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
                    if (lhsValue.Type == typeof(string) && rhsValue.Type == typeof(string))
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.CaseInsensitiveNotEquals(Expr.Arg<string>(), Expr.Arg<string>())));
                    else if (lhsValue.Type == typeof(EntityReference) && rhsValue.Type == typeof(Guid))
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.NotEqual(Expr.Arg<EntityReference>(), Expr.Arg<Guid>())));
                    else if (lhsValue.Type == typeof(Guid) && rhsValue.Type == typeof(EntityReference))
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.NotEqual(Expr.Arg<Guid>(), Expr.Arg<EntityReference>())));
                    else if (lhsValue.Type == typeof(EntityReference) && rhsValue.Type == typeof(EntityReference))
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue, false, Expr.GetMethodInfo(() => ExpressionFunctions.NotEqual(Expr.Arg<EntityReference>(), Expr.Arg<EntityReference>())));
                    else
                        coreComparison = Expression.NotEqual(lhsValue, rhsValue);
                    break;

                default:
                    throw new NotSupportedQueryFragmentException("Unsupported comparison type", fragment);
            }

            // Add null checking for all comparison types
            Expression nullCheck = null;

            if (lhs.Type.IsClass || lhs.Type.IsGenericType && lhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                nullCheck = Expression.Equal(lhs, Expression.Constant(null));

            if (rhs.Type.IsClass || rhs.Type.IsGenericType && rhs.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
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

        /// <summary>
        /// Converts the SELECT clause of a query to FetchXML
        /// </summary>
        /// <param name="select">The SELECT query to convert</param>
        /// <param name="fetch">The FetchXML query converted so far</param>
        /// <param name="tables">The tables involved in the query</param>
        /// <param name="columns">The columns included in the output of the query</param>
        /// <param name="calculatedFields">The functions to use to generate calculated fields</param>
        /// <param name="calculatedFieldExpressions">The expressions that generate the calculated fields</param>
        private string[] HandleSelectClause(QuerySpecification select, FetchXml.FetchType fetch, List<EntityTable> tables, IDictionary<string,Type> calculatedColumns, IList<IQueryExtension> extensions)
        {
            var cols = new List<string>();
            var projections = new Dictionary<string, Func<Entity, object>>();
            
            // Process each column in the SELECT list in turn
            foreach (var field in select.SelectElements)
            {
                if (field is SelectStarExpression star)
                {
                    // Handle SELECT * (i.e. all tables)
                    var starTables = tables;

                    // Handle SELECT table.*
                    if (star.Qualifier != null)
                    {
                        var starTable = FindTable(star.Qualifier.Identifiers.Last().Value, tables, field);

                        if (starTable == null)
                            throw new NotSupportedQueryFragmentException("The column prefix does not match with a table name or alias name used in the query", star.Qualifier);

                        starTables = new List<EntityTable> { starTable };
                    }

                    foreach (var starTable in starTables)
                    {
                        // We need to check the metadata to list all the columns we're going to include in the output dataset. Order these
                        // by name for a more readable result
                        var meta = Metadata[starTable.EntityName];

                        // If we're adding all attributes we can remove individual attributes
                        // FetchXML will ignore an <all-attributes> element if there are any individual <attribute> elements. We can cope
                        // with this by removing the <attribute> elements, but this won't give the expected results if any of the attributes
                        // have aliases
                        if (starTable.GetItems().Any(i => i is FetchAttributeType attr && attr.alias != null))
                        {
                            foreach (var attr in meta.Attributes.Where(a => a.IsValidForRead != false).OrderBy(a => a.LogicalName))
                            {
                                var alias = "";
                                var attrName = AddAttribute(tables, new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier{Value = starTable.Alias},
                                            new Identifier{Value = attr.LogicalName}
                                        }
                                    }
                                }, calculatedColumns, out _, out _, ref alias);

                                if (starTable.LinkEntity == null)
                                    cols.Add(attrName);
                                else
                                    cols.Add((starTable.Alias ?? starTable.EntityName) + "." + attrName);
                            }
                        }
                        else
                        {
                            starTable.RemoveItems(i => i is FetchAttributeType);
                            starTable.AddItem(new allattributes());

                            foreach (var attr in meta.Attributes.Where(a => a.IsValidForRead != false).OrderBy(a => a.LogicalName))
                            {
                                if (starTable.LinkEntity == null)
                                    cols.Add(attr.LogicalName);
                                else
                                    cols.Add((starTable.Alias ?? starTable.EntityName) + "." + attr.LogicalName);
                            }
                        }
                    }
                }
                else if (field is SelectScalarExpression scalar)
                {
                    // Handle SELECT <expression>. Aggregates and groupings will already have been handled, and the SELECT expression replaced with
                    // a temporary field name, e.g SELECT agg1 instead of SELECT min(field)
                    var expr = scalar.Expression;

                    // Get the requested alias for the column, if any.
                    var alias = scalar.ColumnName?.Identifier?.Value;

                    try
                    {
                        var attrName = AddAttribute(tables, expr, calculatedColumns, out var table, out var attr, ref alias);

                        // Even if the attribute wasn't added to the entity because there's already an <all-attributes>, add it to the column list again
                        if (alias == null)
                            cols.Add((table.LinkEntity == null ? "" : ((table.Alias ?? table.EntityName) + ".")) + attrName);
                        else
                            cols.Add(attrName);
                    }
                    catch (NotSupportedQueryFragmentException)
                    {
                        // Try again converting to an expression
                        var func = CompileScalarExpression(expr, tables, calculatedColumns, typeof(object), out var expression);

                        // Calculated field must have an alias, auto-generate one if one is not given
                        if (alias == null)
                            alias = $"Expr{cols.Count + 1}";

                        projections[alias] = func;
                        calculatedColumns[alias] = expression.Type;
                        cols.Add(alias);
                    }
                }
                else
                {
                    // Any other expression type is not supported
                    throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", field);
                }
            }

            if (projections.Count > 0)
                extensions.Add(new Projection(projections));
            
            return cols.ToArray();
        }

        private string AddAttribute(List<EntityTable> tables, ScalarExpression expr, IDictionary<string,Type> calculatedColumns, out EntityTable table, out FetchAttributeType attr, ref string alias)
        {
            if (!(expr is ColumnReferenceExpression col))
                throw new NotSupportedQueryFragmentException("Unhandled SELECT clause", expr);

            // Find the appropriate table and add the attribute to the table
            GetColumnTableAlias(col, tables, out table);
            var attrName = GetColumnAttribute(table, col);
            var requestedAttrName = attrName;

            // Check if this is a column that has already been generated by the GROUP BY clause
            if (col.MultiPartIdentifier.Identifiers.Count == 1 && calculatedColumns != null && calculatedColumns.ContainsKey(attrName))
            {
                attr = tables.SelectMany(t => t.GetItems()).OfType<FetchAttributeType>().First(a => a.alias.Equals(attrName, StringComparison.OrdinalIgnoreCase));
                return requestedAttrName;
            }

            // If this is a virtual attribute, use the underlying attribute instead
            var attrMeta = table.Metadata.Attributes.Single(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

            if (attrMeta == null)
                throw new NotSupportedQueryFragmentException("Unknown attribute", expr);

            if (!String.IsNullOrEmpty(attrMeta.AttributeOf))
                attrName = attrMeta.AttributeOf;

            var matchAnyAlias = alias == "";
            attr = new FetchAttributeType { name = attrName, alias = alias };

            var addAttribute = true;

            if (table.GetItems().Any(i => i is allattributes))
            {
                // If we've already got an <all-attributes> element in this entity, either discard this <attribute> as it will be included
                // in the results anyway or generate an error if it has an alias as we can't combine <all-attributes> and an individual
                // <attribute>
                if (alias == null)
                    addAttribute = false;
                else
                    throw new PostProcessingRequiredException("Cannot add aliased column and wildcard columns from same table", expr);
            }

            // Don't add the attribute if we've already got it
            var newAttr = attr;
            var existingAttr = table.GetItems()
                .OfType<FetchAttributeType>()
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

            if (!String.IsNullOrEmpty(alias))
                return alias;

            return requestedAttrName;
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
        /// <param name="fetch">The FetchXML query to add the &lt;entity&gt; and &lt;link-entity&gt; elements to</param>
        /// <param name="tables">The details of the tables added to the query so far</param>
        private void HandleFromClause(TableReference tableReference, FetchXml.FetchType fetch, List<EntityTable> tables)
        {
            if (tableReference is NamedTableReference namedTable)
            {
                // Handle a single table. First check we don't already have it in our list of tables
                var table = FindTable(namedTable, tables);

                if (table != null)
                    throw new NotSupportedQueryFragmentException("Duplicate table reference", namedTable);

                if (fetch.Items != null)
                    throw new NotSupportedQueryFragmentException("Additional table reference", namedTable);

                // This is the first table in our query, so add it to the root of the FetchXML
                var entity = new FetchEntityType
                {
                    name = namedTable.SchemaObject.BaseIdentifier.Value.ToLower()
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
                    name = table2.SchemaObject.BaseIdentifier.Value.ToLower(),
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

                try
                {
                    HandleFilter(join.SearchCondition, filter, tables, linkTable, null, false, ref col1, ref col2, ref postFilter, null);

                    // We need a join condition comparing a column in the link entity to one in another table
                    if (col1 == null || col2 == null)
                        throw new NotSupportedQueryFragmentException("Missing join condition", join.SearchCondition);
                }
                catch (PostProcessingRequiredException ex)
                {
                    // Check we don't need to apply any custom filter expressions -
                    // we can't do this in the FROM clause as it can break the application of OUTER joins
                    throw new NotSupportedQueryFragmentException("Unsupported join condition - rewrite as WHERE clause", ex.Fragment);
                }

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
        private string GetColumnTableAlias(ColumnReferenceExpression col, List<EntityTable> tables, out EntityTable table, IDictionary<string,Type> calculatedFields = null)
        {
            if (col.MultiPartIdentifier.Identifiers.Count > 2)
                throw new NotSupportedQueryFragmentException("Unsupported column reference", col);

            if (col.MultiPartIdentifier.Identifiers.Count == 2)
            {
                var alias = col.MultiPartIdentifier.Identifiers[0].Value;

                if (alias.Equals(tables[0].Alias ?? tables[0].EntityName, StringComparison.OrdinalIgnoreCase))
                {
                    table = tables[0];
                    ValidateAttributeName(table, col);
                    return null;
                }

                table = tables.SingleOrDefault(t => t.Alias != null && t.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase));
                if (table == null)
                    table = tables.SingleOrDefault(t => t.Alias == null && t.EntityName.Equals(alias, StringComparison.OrdinalIgnoreCase));

                if (table == null)
                    throw new NotSupportedQueryFragmentException("Unknown table " + col.MultiPartIdentifier.Identifiers[0].Value, col);

                ValidateAttributeName(table, col);
                return alias;
            }

            var colName = col.MultiPartIdentifier.Identifiers[0].Value;

            var possibleEntities = tables
                .Where(t => t.GetItems().OfType<FetchAttributeType>().Any(attr => attr.alias?.Equals(colName, StringComparison.OrdinalIgnoreCase) == true))
                .ToArray();

            if (possibleEntities.Length == 0)
            {
                // If no table is explicitly specified, check in the metadata for each available table
                possibleEntities = tables
                    .Where(t => !t.Hidden && t.Metadata.Attributes.Any(attr => attr.LogicalName.Equals(colName, StringComparison.OrdinalIgnoreCase)))
                    .ToArray();
            }

            if (possibleEntities.Length == 0 && colName.EndsWith("utc", StringComparison.OrdinalIgnoreCase))
            {
                // May be using the UTC version of a date/time column
                var baseName = colName.Substring(0, colName.Length - 3);

                possibleEntities = tables
                    .Where(t => !t.Hidden && t.Metadata.Attributes.OfType<DateTimeAttributeMetadata>().Any(attr => attr.LogicalName.Equals(baseName, StringComparison.OrdinalIgnoreCase)))
                    .ToArray();
            }

            if (possibleEntities.Length == 0)
            {
                if (col.MultiPartIdentifier.Identifiers.Count == 1 && calculatedFields != null && calculatedFields.ContainsKey(colName))
                    throw new PostProcessingRequiredException("Calculated field requires post processing", col);

                if (col.MultiPartIdentifier.Identifiers.Last().QuoteType == QuoteType.DoubleQuote && QuotedIdentifiers)
                    throw new NotSupportedQueryFragmentException("Unknown attribute. Double quotes are used to enclose names, single quotes are for strings. Did you mean '" + col.MultiPartIdentifier.Identifiers.Last().Value + "'?", col);

                throw new NotSupportedQueryFragmentException("Unknown attribute", col);
            }

            if (possibleEntities.Length > 1)
                throw new NotSupportedQueryFragmentException("Ambiguous attribute", col);

            table = possibleEntities[0];

            if (possibleEntities[0] == tables[0])
                return null;

            return possibleEntities[0].Alias ?? possibleEntities[0].EntityName;
        }

        private void ValidateAttributeName(EntityTable table, ColumnReferenceExpression col)
        {
            var attrName = GetColumnAttribute(table, col);
            if (!table.Metadata.Attributes.Any(attr => attr.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase)))
                throw new NotSupportedQueryFragmentException("Unknown attribute", col);
        }

        private string GetColumnAttribute(EntityTable table, ColumnReferenceExpression col)
        {
            var attrName = col.MultiPartIdentifier.Identifiers.Last().Value.ToLower();

            if (attrName.EndsWith("utc", StringComparison.OrdinalIgnoreCase))
            {
                var baseName = attrName.Substring(0, attrName.Length - 3);
                var attr = table.Metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(baseName, StringComparison.OrdinalIgnoreCase));

                if (attr != null)
                    attrName = baseName;
            }

            return attrName;
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
