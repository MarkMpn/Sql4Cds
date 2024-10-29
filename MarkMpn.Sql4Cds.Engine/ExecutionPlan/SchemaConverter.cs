using System;
using System.Data;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class SchemaConverter
    {
        public static INodeSchema ConvertSchema(DataTable schemaTable, DataSource dataSource, QuerySpecification querySpec = null)
        {
            var columns = new ColumnList();

            foreach (DataRow column in schemaTable.Rows)
            {
                var columnName = (string)column["ColumnName"];
                var dataTypeName = (string)column["DataTypeName"];
                var nullable = (bool)column["AllowDBNull"];
                var isCalculated = column.IsNull("IsExpression") ? false : (bool)column["IsExpression"];
                var size = (int)column["ColumnSize"];
                var scale = (short)column["NumericScale"];
                var precision = (short)column["NumericPrecision"];

                DataTypeReference dataType;

                // Convert the type details from the schema table to the ScriptDom type
                if (Enum.TryParse<SqlDataTypeOption>(dataTypeName, true, out var sqlDataTypeOption))
                {
                    var sqlDataType = new SqlDataTypeReference { SqlDataTypeOption = sqlDataTypeOption };
                    dataType = sqlDataType;

                    if (sqlDataTypeOption.IsStringType())
                    {
                        sqlDataType = new SqlDataTypeReferenceWithCollation
                        {
                            SqlDataTypeOption = sqlDataTypeOption,
                            Collation = dataSource.DefaultCollation,
                            CollationLabel = CollationLabel.Implicit
                        };
                        dataType = sqlDataType;

                        if (size >= 4000)
                            sqlDataType.Parameters.Add(new MaxLiteral());
                        else
                            sqlDataType.Parameters.Add(new IntegerLiteral { Value = size.ToString() });
                    }
                    else if (sqlDataTypeOption == SqlDataTypeOption.Decimal || sqlDataTypeOption == SqlDataTypeOption.Numeric)
                    {
                        sqlDataType.Parameters.Add(new IntegerLiteral { Value = precision.ToString() });
                        sqlDataType.Parameters.Add(new IntegerLiteral { Value = scale.ToString() });
                    }
                    else if (sqlDataTypeOption == SqlDataTypeOption.DateTime2 || sqlDataTypeOption == SqlDataTypeOption.DateTimeOffset || sqlDataTypeOption == SqlDataTypeOption.Time)
                    {
                        sqlDataType.Parameters.Add(new IntegerLiteral { Value = scale.ToString() });
                    }
                }
                else if (dataTypeName.Equals("xml", StringComparison.OrdinalIgnoreCase))
                {
                    dataType = DataTypeHelpers.Xml;
                }
                else
                {
                    dataType = new UserDataTypeReference { Name = new SchemaObjectName { Identifiers = { new Identifier { Value = dataTypeName } } } };
                }

                // NULL literals are assumed to be integers. Check if we can identify that this is a null literal
                // so we can implicitly convert it to any other type if required.
                if (querySpec != null && dataType.IsSameAs(DataTypeHelpers.Int))
                {
                    var colIndex = columns.Count;

                    for (var i = 0; i <= colIndex && i < querySpec.SelectElements.Count; i++)
                    {
                        if (!(querySpec.SelectElements[i] is SelectScalarExpression sse))
                            break;

                        if (i == colIndex && sse.Expression is NullLiteral)
                            dataType = DataTypeHelpers.ImplicitIntForNullLiteral;
                    }
                }

                if (String.IsNullOrEmpty(columnName))
                    columnName = columns.Count.ToString();

                columns[columnName] = new ColumnDefinition(dataType, nullable, isCalculated);
            }

            return new NodeSchema(columns, null, null, null);
        }

    }
}
