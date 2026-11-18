using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Generates SQL templates for tables, table-valued functions and stored procedures.
    /// </summary>
    /// <remarks>
    /// Callers supply full entity or message metadata and their preferred column ordering.
    /// </remarks>
    public static class ScriptGenerator
    {
        /// <summary>
        /// Generates a table SELECT template, optionally limited to a number of rows.
        /// </summary>
        public static string GenerateSelectQuery(string schema, EntityMetadata metadata, int? top, DataSource dataSource, ColumnOrdering columnOrdering)
        {
            var attrs = metadata.Attributes.Where(attribute => attribute.IsValidForRead != false && attribute.AttributeOf == null);
            var sortedAttrs = columnOrdering == ColumnOrdering.Strict
                ? attrs.OrderBy(attribute => attribute.ColumnNumber)
                : attrs.OrderBy(attribute => attribute.LogicalName);
            var columns = sortedAttrs.SelectMany(attribute =>
                new[] { attribute.LogicalName }.Concat(attribute.GetVirtualAttributes(metadata, dataSource, false).Select(virtualAttribute => attribute.LogicalName + virtualAttribute.Suffix)));

            var query = new QuerySpecification();
            if (top.HasValue)
                query.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = top.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) } };
            foreach (var column in columns)
                query.SelectElements.Add(new SelectScalarExpression { Expression = Column(column) });
            query.FromClause = new FromClause
            {
                TableReferences =
                {
                    new NamedTableReference
                    {
                        SchemaObject = new SchemaObjectName
                        {
                            Identifiers = { new Identifier { Value = schema }, new Identifier { Value = metadata.LogicalName } }
                        }
                    }
                }
            };
            return query.ToSql();
        }

        /// <summary>
        /// Generates a table-valued function SELECT template with typed input placeholders.
        /// </summary>
        public static string GenerateSelectQuery(Message message, DataSource dataSource, ColumnOrdering columnOrdering)
        {
            var query = new QuerySpecification();
            query.SelectElements.Add(new SelectScalarExpression { Expression = new ColumnReferenceExpression { ColumnType = ColumnType.Wildcard } });
            var function = new SchemaObjectFunctionTableReference
            {
                SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = message.Name } } }
            };
            query.FromClause = new FromClause { TableReferences = { function } };

            foreach (var parameter in OrderParameters(message.InputParameters.Where(parameter => parameter.Type != typeof(PagingInfo)), columnOrdering))
                function.Parameters.Add(new VariableReference { Name = $"<@{parameter.Name}, {parameter.GetSqlDataType(dataSource).ToSql()}>" });

            return query.ToSql();
        }

        /// <summary>
        /// Generates an INSERT template for createable columns with typed value placeholders.
        /// </summary>
        public static string GenerateInsertQuery(string schema, EntityMetadata metadata, DataSource dataSource, ColumnOrdering columnOrdering)
        {
            var columns = GetWriteColumns(metadata, dataSource, columnOrdering, create: true);
            var insert = new InsertStatement
            {
                InsertSpecification = new InsertSpecification
                {
                    Target = Table(schema, metadata.LogicalName),
                    InsertSource = new ValuesInsertSource()
                }
            };
            var row = new RowValue();
            ((ValuesInsertSource)insert.InsertSpecification.InsertSource).RowValues.Add(row);
            foreach (var column in columns)
            {
                insert.InsertSpecification.Columns.Add(Column(column.Name));
                row.ColumnValues.Add(Column($"<{column.Name}, {column.Type.ToSql()}>"));
            }
            var sql = insert.ToSql();
            return sql.StartsWith("INSERT  ", StringComparison.Ordinal) ? sql.Remove(7, 1) : sql;
        }

        /// <summary>
        /// Generates an UPDATE template for updateable columns with typed value placeholders.
        /// </summary>
        public static string GenerateUpdateQuery(string schema, EntityMetadata metadata, DataSource dataSource, ColumnOrdering columnOrdering)
        {
            var update = new UpdateStatement
            {
                UpdateSpecification = new UpdateSpecification { Target = Table(schema, metadata.LogicalName) }
            };
            foreach (var column in GetWriteColumns(metadata, dataSource, columnOrdering, create: false))
                update.UpdateSpecification.SetClauses.Add(new AssignmentSetClause
                {
                    Column = Column(column.Name),
                    NewValue = Column($"<{column.Name}, {column.Type.ToSql()}>")
                });
            return update.ToSql() + "\r\nWHERE <Search Conditions>";
        }

        private static IEnumerable<(string Name, DataTypeReference Type)> GetWriteColumns(
            EntityMetadata metadata,
            DataSource dataSource,
            ColumnOrdering columnOrdering,
            bool create)
        {
            var attributes = metadata.Attributes.Where(attribute =>
                (create ? attribute.IsValidForCreate : attribute.IsValidForUpdate) != false && attribute.AttributeOf == null);
            var ordered = columnOrdering == ColumnOrdering.Strict
                ? attributes.OrderBy(attribute => attribute.ColumnNumber)
                : attributes.OrderBy(attribute => attribute.LogicalName);
            foreach (var attribute in ordered)
            {
                var type = attribute.GetAttributeSqlType(dataSource, true);
                if (type is UserDataTypeReference userType && userType.Name.BaseIdentifier.Value == "EntityReference")
                    type = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };
                yield return (attribute.LogicalName, type);
                foreach (var virtualAttribute in attribute.GetVirtualAttributes(metadata, dataSource, true))
                    yield return (attribute.LogicalName + virtualAttribute.Suffix, virtualAttribute.DataType());
            }
        }

        /// <summary>
        /// Generates a DELETE template with a search condition placeholder.
        /// </summary>
        public static string GenerateDeleteQuery(string schema, string name) =>
            $"DELETE FROM {schema}.{name}\r\nWHERE <Search Conditions>";

        /// <summary>
        /// Generates an EXECUTE template declaring input, output and return-value variables.
        /// </summary>
        public static string GenerateExecuteQuery(Message message, DataSource dataSource, ColumnOrdering columnOrdering)
        {
            var parameters = OrderParameters(message.InputParameters.Where(parameter => parameter.Type != typeof(PagingInfo)), columnOrdering).ToArray();
            var batch = new TSqlBatch();
            var declare = new DeclareVariableStatement();
            batch.Statements.Add(declare);
            declare.Declarations.Add(new DeclareVariableElement
            {
                VariableName = new Identifier { Value = "@RC" },
                DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int }
            });
            foreach (var parameter in parameters.Concat(message.OutputParameters))
                declare.Declarations.Add(new DeclareVariableElement
                {
                    VariableName = new Identifier { Value = $"@{parameter.Name}" },
                    DataType = parameter.GetSqlDataType(dataSource)
                });
            var execute = new ExecuteStatement
            {
                ExecuteSpecification = new ExecuteSpecification
                {
                    Variable = new VariableReference { Name = "@RC" },
                    ExecutableEntity = new ExecutableProcedureReference
                    {
                        ProcedureReference = new ProcedureReferenceName
                        {
                            ProcedureReference = new ProcedureReference
                            {
                                Name = new SchemaObjectName
                                {
                                    Identifiers = { new Identifier { Value = "dbo" }, new Identifier { Value = message.Name } }
                                }
                            }
                        }
                    }
                }
            };
            foreach (var parameter in parameters)
                execute.ExecuteSpecification.ExecutableEntity.Parameters.Add(new ExecuteParameter
                {
                    Variable = new VariableReference { Name = $"@{parameter.Name}" },
                    ParameterValue = new VariableReference { Name = $"@{parameter.Name}" }
                });
            foreach (var parameter in message.OutputParameters)
                execute.ExecuteSpecification.ExecutableEntity.Parameters.Add(new ExecuteParameter
                {
                    Variable = new VariableReference { Name = $"@{parameter.Name}" },
                    ParameterValue = new VariableReference { Name = $"@{parameter.Name}" },
                    IsOutput = true
                });
            batch.Statements.Add(execute);
            return batch.ToSql();
        }

        /// <summary>
        /// Generates a SELECT statement to retrieve a single record, qualifying the table with the record's
        /// instance name when it differs from the current connection.
        /// </summary>
        /// <param name="currentDataSource">The name of the instance the query will be executed against</param>
        /// <param name="recordDataSource">The name of the instance the record belongs to</param>
        /// <param name="metadata">The metadata of the table the record belongs to</param>
        /// <param name="id">The unique identifier of the record</param>
        public static string GenerateRecordSelectQuery(string currentDataSource, string recordDataSource, EntityMetadata metadata, Guid id)
        {
            var table = Autocomplete.SqlAutocompleteItem.EscapeIdentifier(metadata.LogicalName);

            if (!String.IsNullOrEmpty(recordDataSource) && !String.Equals(currentDataSource, recordDataSource, StringComparison.OrdinalIgnoreCase))
                table = $"{Autocomplete.SqlAutocompleteItem.EscapeIdentifier(recordDataSource)}.dbo.{table}";

            return $"SELECT * FROM {table} WHERE {Autocomplete.SqlAutocompleteItem.EscapeIdentifier(metadata.PrimaryIdAttribute)} = '{id}'";
        }

        private static IEnumerable<MessageParameter> OrderParameters(IEnumerable<MessageParameter> parameters, ColumnOrdering columnOrdering) =>
            columnOrdering == ColumnOrdering.Alphabetical
                ? parameters.OrderBy(parameter => parameter.Name, StringComparer.OrdinalIgnoreCase)
                : parameters.OrderBy(parameter => parameter.Position);

        private static NamedTableReference Table(string schema, string name) => new NamedTableReference
        {
            SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = schema }, new Identifier { Value = name } } }
        };

        private static ColumnReferenceExpression Column(string name) => new ColumnReferenceExpression
        {
            MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = name } } }
        };
    }
}
