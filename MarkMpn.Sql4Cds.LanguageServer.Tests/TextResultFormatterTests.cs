using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Export;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class TextResultFormatterTests
    {
        private static DataTable Schema(params (string name, string type, int size, short precision, short scale)[] columns)
        {
            var schema = new DataTable();
            schema.Columns.Add("ColumnName", typeof(string));
            schema.Columns.Add("ColumnSize", typeof(int));
            schema.Columns.Add("NumericPrecision", typeof(short));
            schema.Columns.Add("NumericScale", typeof(short));
            schema.Columns.Add("DataTypeName", typeof(string));

            foreach (var column in columns)
                schema.Rows.Add(column.name, column.size, column.precision, column.scale, column.type);

            return schema;
        }

        [TestMethod]
        public void ColumnWidthsFollowDataTypes()
        {
            var formatter = new TextResultFormatter(Schema(
                ("name", "nvarchar", 100, 255, 255),
                ("description", "nvarchar", Int32.MaxValue, 255, 255),
                ("accountid", "uniqueidentifier", 16, 255, 255),
                ("flag", "bit", 1, 255, 255),
                ("amount", "decimal", 17, 23, 2),
                ("createdon", "datetime", 8, 23, 3),
                ("variant", "sql_variant", 8016, 255, 255)), false);

            CollectionAssert.AreEqual(new[] { 100, 256, 36, 4, 24, 23, 256 }, formatter.ColumnWidths.ToArray());
        }

        [TestMethod]
        public void FormatsHeaderAndRows()
        {
            var formatter = new TextResultFormatter(Schema(
                ("name", "nvarchar", 6, 255, 255),
                ("employees", "int", 4, 10, 255),
                ("active", "bit", 1, 255, 255)), false);

            CollectionAssert.AreEqual(new[]
            {
                "name   employees   active",
                "------ ----------- ------"
            }, formatter.GetHeaderLines().ToArray());

            Assert.AreEqual("Data8  12          1     ", formatter.FormatRow(new object[] { "Data8", 12, true }));
            Assert.AreEqual("NULL   NULL        0     ", formatter.FormatRow(new object[] { DBNull.Value, null, false }));
        }

        [TestMethod]
        public void TruncatesLongValues()
        {
            var formatter = new TextResultFormatter(Schema(("code", "nvarchar", 4, 255, 255)), false);

            Assert.AreEqual("abcd", formatter.FormatRow(new object[] { "abcdefgh" }));
        }

        [TestMethod]
        public async Task SendsRowsInBatches()
        {
            var formatter = new TextResultFormatter(Schema(("id", "int", 4, 10, 255)), false);
            var data = new DataTable();
            data.Columns.Add("id", typeof(int));

            for (var i = 1; i <= 5; i++)
                data.Rows.Add(i);

            var messages = new List<string>();
            var rowCounts = new List<int>();

            using (var reader = data.CreateDataReader())
                await QueryExecutionHandler.SendTextResultAsync(reader, formatter, true, (msg, rows) => { messages.Add(msg); rowCounts.Add(rows); return Task.CompletedTask; }, batchSize: 4);

            CollectionAssert.AreEqual(new[]
            {
                "\nid         \n-----------\n1          ",
                "2          \n3          \n4          \n5          "
            }, messages);
            CollectionAssert.AreEqual(new[] { 1, 4 }, rowCounts);
        }
    }
}
