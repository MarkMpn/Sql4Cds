using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Xml.Linq;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.Export.DataStorage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class ExcelExportTests
    {
        private static readonly XNamespace Spreadsheet = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

        [TestMethod]
        public void AutoSizingUsesTheWidestHeaderOrDataValue()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams { AutoSizeColumns = true, IncludeHeaders = true },
                new[] { "Header", "B" }, new[] { "x", "1234567890" }, new[] { "xx", "y" });
            var widths = Widths(sheet);
            Assert.AreEqual(ToExcelWidth(6 * 7), widths[0]);
            Assert.AreEqual(ToExcelWidth(10 * 7), widths[1]);
            CollectionAssert.AreEqual(new[] { "Header", "B", "x", "1234567890", "xx", "y" },
                sheet.Descendants(Spreadsheet + "t").Select(text => text.Value).ToArray());
        }

        [TestMethod]
        public void BoldHeadersAndAutoFiltersRetainTheirExtraWidthAndFormatting()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams
            {
                AutoSizeColumns = true,
                IncludeHeaders = true,
                BoldHeaderRow = true,
                AutoFilterHeaderRow = true,
                FreezeHeaderRow = true
            }, new[] { "Header" }, new[] { "x" });
            Assert.AreEqual(ToExcelWidth(6 * 7 * 1.1F + 17), Widths(sheet)[0]);
            Assert.AreEqual("1", (string)sheet.Descendants(Spreadsheet + "pane").Single().Attribute("ySplit"));
            Assert.AreEqual("A1:A1", (string)sheet.Descendants(Spreadsheet + "autoFilter").Single().Attribute("ref"));
            Assert.AreEqual("5", (string)sheet.Descendants(Spreadsheet + "c").First().Attribute("s"));
        }

        [TestMethod]
        public void MultilineValuesUseTheLongestLineAndTabsHaveAnEstimatedWidth()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams { AutoSizeColumns = true },
                new[] { "Ignored long header", "Other" }, new[] { "abc\r\ndefgh\nij\rk", "a\tb" });
            CollectionAssert.AreEqual(new[] { ToExcelWidth(5 * 7), ToExcelWidth(6 * 7) }, Widths(sheet));
            Assert.AreEqual(1, sheet.Descendants(Spreadsheet + "row").Count());
        }

        [TestMethod]
        public void EmptyValuesHavePaddingAndLongValuesRespectExcelWidthLimit()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams { AutoSizeColumns = true },
                new[] { "Empty", "Null", "Long" }, new[] { "", null, new string('x', 1000) });
            CollectionAssert.AreEqual(new[] { ToExcelWidth(0), ToExcelWidth(0), 255D }, Widths(sheet));
        }

        [TestMethod]
        public void AutoSizingCanBeDisabledWithoutChangingCellData()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams { IncludeHeaders = true },
                new[] { "Name" }, new[] { "Value" });
            Assert.AreEqual(0, Widths(sheet).Length);
            CollectionAssert.AreEqual(new[] { "Name", "Value" },
                sheet.Descendants(Spreadsheet + "t").Select(text => text.Value).ToArray());
        }

        [TestMethod]
        public void SelectedColumnsAreMeasuredAndWrittenWithoutUnselectedColumns()
        {
            var sheet = Export(new SaveResultsAsExcelRequestParams
            {
                AutoSizeColumns = true,
                IncludeHeaders = true,
                ColumnStartIndex = 1,
                ColumnEndIndex = 1,
                RowStartIndex = 0,
                RowEndIndex = 0
            }, new[] { "Ignore", "Selected", "Ignore too" }, new[] { "Ignored long value", "x", "Ignore" });
            CollectionAssert.AreEqual(new[] { ToExcelWidth(8 * 7) }, Widths(sheet));
            CollectionAssert.AreEqual(new[] { "Selected", "x" },
                sheet.Descendants(Spreadsheet + "t").Select(text => text.Value).ToArray());
        }

        private static XDocument Export(SaveResultsAsExcelRequestParams options, string[] headers, params string[][] values)
        {
            var columns = headers.Select(header => new DbColumnWrapper(header, "nvarchar", null)).ToArray();
            var rows = values.Select(row => row.Select(value => new DbCellValue
            {
                DisplayValue = value,
                RawObject = value,
                IsNull = value == null
            }).ToArray()).ToArray();
            using var stream = new MemoryStream();
            using (var writer = new SaveAsExcelFileStreamWriter(stream, options, columns, null))
            {
                if (writer.ShouldMeasureRowColumns)
                {
                    foreach (var row in rows)
                        writer.MeasureRowColumns(row);
                }
                foreach (var row in rows)
                    writer.WriteRow(row, columns);
            }
            using var archiveStream = new MemoryStream(stream.ToArray());
            using var archive = new ZipArchive(archiveStream, ZipArchiveMode.Read);
            using var sheetStream = archive.GetEntry("xl/worksheets/sheet1.xml").Open();
            return XDocument.Load(sheetStream);
        }

        private static double[] Widths(XDocument sheet)
        {
            return sheet.Descendants(Spreadsheet + "col")
                .Select(column => Double.Parse(column.Attribute("width").Value, CultureInfo.InvariantCulture)).ToArray();
        }

        private static double ToExcelWidth(float pixels)
        {
            return Math.Truncate((pixels + 15) / 7F * 256) / 256;
        }
    }
}
