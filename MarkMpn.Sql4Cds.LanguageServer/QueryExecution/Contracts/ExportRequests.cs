using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    internal static class ExportRequests
    {
        public const string CsvMessageName = "query/saveCsv";
        public const string ExcelMessageName = "query/saveExcel";
        public const string JsonMessageName = "query/saveJson";
        public const string MarkdownMessageName = "query/saveMarkdown";
        public const string XmlMessageName = "query/saveXml";

        public static readonly LspRequest<SaveResultsAsCsvRequestParams, SaveResultRequestResult> CsvType = new LspRequest<SaveResultsAsCsvRequestParams, SaveResultRequestResult>(CsvMessageName);
        public static readonly LspRequest<SaveResultsAsExcelRequestParams, SaveResultRequestResult> ExcelType = new LspRequest<SaveResultsAsExcelRequestParams, SaveResultRequestResult>(ExcelMessageName);
        public static readonly LspRequest<SaveResultsAsJsonRequestParams, SaveResultRequestResult> JsonType = new LspRequest<SaveResultsAsJsonRequestParams, SaveResultRequestResult>(JsonMessageName);
        public static readonly LspRequest<SaveResultsAsMarkdownRequestParams, SaveResultRequestResult> MarkdownType = new LspRequest<SaveResultsAsMarkdownRequestParams, SaveResultRequestResult>(MarkdownMessageName);
        public static readonly LspRequest<SaveResultsAsXmlRequestParams, SaveResultRequestResult> XmlType = new LspRequest<SaveResultsAsXmlRequestParams, SaveResultRequestResult>(XmlMessageName);
    }

    /// <summary>
    /// Parameters for the save results result
    /// </summary>
    public class SaveResultRequestResult
    {
        /// <summary>
        /// Error messages for saving to file. 
        /// </summary>
        public string Messages { get; set; }
    }
}
