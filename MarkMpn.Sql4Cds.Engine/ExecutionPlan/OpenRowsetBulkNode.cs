using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenRowsetBulkNode : BaseDataNode
    {
        private const string BulkColumnName = "BulkColumn";
        private const string ContextColumnName = "@CONTEXT";
        private static readonly DataTypeReference ContextColumnType = DataTypeHelpers.Object(typeof(OpenRowsetBulkContext));

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
        public string DataFileFormat { get; set; }

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

        /// <summary>
        /// The first row to read from the file (1-based)
        /// </summary>
        [Category("OpenRowset")]
        [Description("The first row to read from the file (1-based)")]
        public int FirstRow { get; set; }

        /// <summary>
        /// The last row to read from the file (1-based)
        /// </summary>
        [Category("OpenRowset")]
        [Description("The last row to read from the file (1-based)")]
        public int LastRow { get; set; }

        /// <summary>
        /// Specifies the code page of the data in the data file
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies the code page of the data in the data file")]
        public string CodePage { get; set; }

        /// <summary>
        /// Specifies the row terminator to be used
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies the row terminator to be used")]
        public string RowTerminator { get; set; } = "\r\n";

        /// <summary>
        /// Specifies the field terminator to be used
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies the field terminator to be used")]
        public string FieldTerminator { get; set; } = ",";

        /// <summary>
        /// Specifies a character that is used as the quote character in the CSV file
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies a character that is used as the quote character in the CSV file")]
        public string FieldQuote { get; set; } = "\"";

        /// <summary>
        /// Specifies a character that is used to escape the quote character in the CSV file
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies a character that is used to escape the quote character in the CSV file")]
        public string EscapeChar { get; set; } = "\"";

        /// <summary>
        /// Specifies the maximum number of syntax errors or nonconforming rows, which can occur before OPENROWSET throws an exception
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies the maximum number of syntax errors or nonconforming rows, which can occur before OPENROWSET throws an exception")]
        public int MaxErrors { get; set; } = 10;

        /// <summary>
        /// Specifies the file used to collect rows that have formatting errors
        /// </summary>
        [Category("OpenRowset")]
        [Description("Specifies the file used to collect rows that have formatting errors")]
        public string ErrorFile { get; set; }

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

            // Add a hidden @CONTEXT column to expose filename information to subsequent nodes
            schema.Add(alias + "." + ContextColumnName, new ColumnDefinition(ContextColumnType, false, false, isVisible: false));

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

                        using (var reader = new StreamReader(stream, GetEncoding()))
                        {
                            var value = reader.ReadToEnd();
                            var sqlString = SqlTypeConverter.NetToSqlType(context.PrimaryDataSource, value, type);
                            record[escapedAlias + "." + BulkColumnName] = sqlString;
                        }
                    }
                }

                record[escapedAlias + "." + ContextColumnName] = new OpenRowsetBulkContext { Filename = Filename, FilenamePattern = Filename };

                yield return record;
            }
            else
            {
                var dir = Path.GetDirectoryName(Filename);
                var file = Path.GetFileName(Filename);
                dir = Path.Combine(Environment.CurrentDirectory, dir);
                var nvarcharmax = DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault);
                var eec = new ExpressionExecutionContext(context);

                var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture);
                csvConfig.NewLine = ParseTerminator(RowTerminator);
                csvConfig.Delimiter = ParseTerminator(FieldTerminator);
                csvConfig.Quote = FieldQuote[0];
                csvConfig.Escape = EscapeChar[0];

                var errors = 0;

                using (var errorsWriter = OpenWriter(ErrorFile))
                using (var logWriter = OpenWriter(String.IsNullOrEmpty(ErrorFile) ? ErrorFile : Path.ChangeExtension(ErrorFile, ".ERROR.txt")))
                {
                    foreach (var filename in EnumerateFiles(dir, file))
                    {
                        var filenameContext = new OpenRowsetBulkContext { Filename = filename, FilenamePattern = Filename };

                        using (var reader = new StreamReader(OpenFile(filename), GetEncoding()))
                        using (var csv = new CsvParser(reader, csvConfig))
                        {
                            var rowNum = 0;

                            while (csv.Read())
                            {
                                rowNum++;

                                if (rowNum < FirstRow)
                                    continue;

                                if (LastRow > 0 && rowNum > LastRow)
                                    break;

                                var record = new Entity();
                                var valid = true;

                                try
                                {
                                    for (var i = 0; i < Schema.Count; i++)
                                    {
                                        var col = Schema[i];
                                        var escapedName = col.ColumnIdentifier.Value.EscapeIdentifier();
                                        var value = csv[i];

                                        // TODO: If we're using the RAW code page and loading into a string column, we need to
                                        // re-interpret the bytes we've loaded into the string using the encoding for this column.

                                        // Convert the value to a SqlString, then to the target type
                                        var sqlValue = SqlTypeConverter.NetToSqlType(context.PrimaryDataSource, value, nvarcharmax);
                                        sqlValue = SqlTypeConverter.GetConversion(nvarcharmax, col.DataType)(sqlValue, eec);

                                        record[escapedAlias + "." + escapedName] = sqlValue;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    errors++;
                                    valid = false;

                                    if (errorsWriter != null)
                                        errorsWriter.Write(csv.RawRecord);

                                    if (logWriter != null)
                                        logWriter.WriteLine($"File: {filename}, Row: {rowNum}, Error: {ex.Message}");

                                    if (errors == MaxErrors)
                                        throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkMaxErrorsReached(MaxErrors), ex);
                                }

                                if (valid)
                                {
                                    record[escapedAlias + "." + ContextColumnName] = filenameContext;
                                    yield return record;
                                }
                            }
                        }
                    }
                }
            }
        }

        private IEnumerable<string> EnumerateFiles(string dir, string file)
        {
            if (!file.Contains("*"))
                return new[] { Path.Combine(dir, file) };
            
            try
            {
                return Directory.EnumerateFiles(dir, file);
            }
            catch (ArgumentException ex)
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkFileDoesNotExist(Filename), ex);
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkFileDoesNotExistOrOpen(Filename), ex);
            }
        }

        private Stream OpenFile(string filename)
        {
            try
            {
                return File.OpenRead(filename);
            }
            catch (ArgumentException ex)
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkFileDoesNotExist(Filename), ex);
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkFileDoesNotExistOrOpen(Filename), ex);
            }
        }

        private StreamWriter OpenWriter(string filename)
        {
            if (String.IsNullOrEmpty(filename))
                return null;

            try
            {
                var stream = File.Open(filename, FileMode.CreateNew, FileAccess.Write);
                return new StreamWriter(stream, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(Sql4CdsError.OpenRowsetBulkErrorWritingFile(filename, ex.Message), ex);
            }
        }

        private Encoding GetEncoding()
        {
            switch (CodePage)
            {
                case "ACP":
                    return Encoding.GetEncoding(1252);

                case "OEM":
                case null:
                    return Encoding.GetEncoding(CultureInfo.CurrentCulture.TextInfo.OEMCodePage);

                case "RAW":
                    return Encoding.ASCII;

                default:
                    return Encoding.GetEncoding(Int32.Parse(CodePage));
            }
        }

        private string ParseTerminator(string terminator)
        {
            return terminator
                .Replace("\\t", "\t")
                .Replace("\\n", "\n")
                .Replace("\\r", "\r")
                .Replace("\\0", "\0");
        }

        public override object Clone()
        {
            var clone = new OpenRowsetBulkNode();
            clone.Filename = Filename;
            clone.Alias = Alias;
            clone.DataFileFormat = DataFileFormat;
            clone.SingleOption = SingleOption;
            clone.Schema = Schema;
            clone.FirstRow = FirstRow;
            clone.LastRow = LastRow;
            clone.CodePage = CodePage;
            clone.RowTerminator = RowTerminator;
            clone.FieldTerminator = FieldTerminator;
            clone.FieldQuote = FieldQuote;
            clone.EscapeChar = EscapeChar;
            clone.MaxErrors = MaxErrors;
            clone.ErrorFile = ErrorFile;
            return clone;
        }
    }

    class OpenRowsetBulkContext : INullable
    {
        private Regex _filenameRegex;

        public string Filename { get; set; }

        public string FilenamePattern { get; set; }

        public Regex FilenameRegex
        {
            get
            {
                if (_filenameRegex == null)
                {
                    var regex = Regex.Escape(FilenamePattern)
                        .Replace("\\*\\*$", "(.*)")
                        .Replace("\\*", "(.*)");

                    _filenameRegex = new Regex("^" + regex + "$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
                }

                return _filenameRegex;
            }
        }

        public bool IsNull { get; set; }

        public static OpenRowsetBulkContext Null => new OpenRowsetBulkContext { IsNull = true };
    }
}
