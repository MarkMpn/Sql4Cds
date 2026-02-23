using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenRowsetBulkNode : BaseDataNode
    {
        private const string BulkColumnName = "BulkColumn";

        /// <summary>
        /// The path of the file to open
        /// </summary>
        [Category("OpenRowset")]
        [Description("The path of the file to open")]
        public string Filename { get; set; }

        /// <summary>
        /// The alias to apply to the results of the OPENROWSET function
        /// </summary>
        [Category("OpenRowset")]
        [Description("The alias to apply to the results of the OPENROWSET function")]
        public string Alias { get; set; }

        /// <summary>
        /// The format of the file to load
        /// </summary>
        [Category("OpenRowset")]
        [Description("The format of the file to load")]
        public string Format { get; set; }

        /// <summary>
        /// Indicates if the file is to be read as a single value
        /// </summary>
        [Category("OpenRowset")]
        [Description("Indicates if the file is to be read as a single value")]
        public BulkInsertOptionKind? SingleOption { get; set; }

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        [Browsable(false)]
        public IList<OpenRowsetColumnDefinition> Schema { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new ColumnList();
            var alias = Alias.EscapeIdentifier();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            if (SingleOption == BulkInsertOptionKind.SingleBlob)
            {
                schema.Add(alias + "." + BulkColumnName, new ColumnDefinition(DataTypeHelpers.VarBinary(Int32.MaxValue), false, false, isWildcardable: true));
                aliases.Add(BulkColumnName, new[] { alias + "." + BulkColumnName });
            }
            else if (SingleOption == BulkInsertOptionKind.SingleClob)
            {
                schema.Add(alias + "." + BulkColumnName, new ColumnDefinition(DataTypeHelpers.VarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault), false, false, isWildcardable: true));
                aliases.Add(BulkColumnName, new[] { alias + "." + BulkColumnName });
            }
            else if (SingleOption == BulkInsertOptionKind.SingleNClob)
            {
                schema.Add(alias + "." + BulkColumnName, new ColumnDefinition(DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault), false, false, isWildcardable: true));
                aliases.Add(BulkColumnName, new[] { alias + "." + BulkColumnName });
            }
            else
            {
                foreach (var col in Schema)
                {
                    var escapedName = col.ColumnIdentifier.Value.EscapeIdentifier();
                    schema.Add(alias + "." + escapedName, new ColumnDefinition(col.DataType, true, false, isWildcardable: true));
                    aliases.Add(escapedName, new[] { alias + "." + escapedName });
                }
            }

            return new NodeSchema(
                schema,
                aliases,
                null,
                null);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            throw new NotImplementedException();
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var escapedAlias = Alias.EscapeIdentifier();

            if (SingleOption != null)
            {
                var record = new Entity();

                using (var stream = OpenFile(Filename))
                {
                    if (SingleOption == BulkInsertOptionKind.SingleBlob)
                    {
                        using (var memoryStream = new MemoryStream())
                        {
                            stream.CopyTo(memoryStream);
                            record[escapedAlias + "." + BulkColumnName] = new SqlBinary(memoryStream.ToArray());
                        }
                    }
                    else
                    {
                        var type = SingleOption == BulkInsertOptionKind.SingleClob 
                            ? DataTypeHelpers.VarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault) 
                            : DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault);

                        using (var reader = new StreamReader(stream))
                        {
                            var value = reader.ReadToEnd();
                            var sqlString = SqlTypeConverter.NetToSqlType(context.PrimaryDataSource, value, type);
                            record[escapedAlias + "." + BulkColumnName] = sqlString;
                        }
                    }
                }

                yield return record;
            }
        }

        private Stream OpenFile(string filename)
        {
            try
            {
                return File.OpenRead(filename);
            }
            catch
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkFileDoesNotExist(Filename));
            }
        }

        public override object Clone()
        {
            var clone = new OpenRowsetBulkNode();
            clone.Filename = Filename;
            clone.Alias = Alias;
            clone.Format = Format;
            clone.SingleOption = SingleOption;
            clone.Schema = Schema;
            return clone;
        }
    }
}
