//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//


namespace MarkMpn.Sql4Cds.Export.Contracts
{
    /// <summary>
    /// Parameters for the save results request
    /// </summary>
    public class SaveResultsRequestParams
    {
        /// <summary>
        /// The path of the file to save results in
        /// </summary>
        public string FilePath { get; set; }

        /// <summary>
        /// Index of the batch to get the results from
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Index of the result set to get the results from
        /// </summary>
        public int ResultSetIndex { get; set; }

        /// <summary>
        /// URI for the editor that called save results
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Start index of the selected rows (inclusive)
        /// </summary>
        public int? RowStartIndex { get; set; }

        /// <summary>
        /// End index of the selected rows (inclusive)
        /// </summary>
        public int? RowEndIndex { get; set; }
        
        /// <summary>
        /// Start index of the selected columns (inclusive)
        /// </summary>
        /// <returns></returns>
        public int? ColumnStartIndex { get; set; }

        /// <summary>
        /// End index of the selected columns (inclusive)
        /// </summary>
        /// <returns></returns>
        public int? ColumnEndIndex { get; set; }

        /// <summary>
        /// Check if request is a subset of result set or whole result set
        /// </summary>
        /// <returns></returns>
        internal bool IsSaveSelection
        {
            get
            {
                return ColumnStartIndex.HasValue && ColumnEndIndex.HasValue
                       && RowStartIndex.HasValue && RowEndIndex.HasValue;
            }
        }
    }

    /// <summary>
    /// Parameters to save results as CSV
    /// </summary>
    public class SaveResultsAsCsvRequestParams: SaveResultsRequestParams
    {
        /// <summary>
        /// Include headers of columns in CSV
        /// </summary>
        public bool IncludeHeaders { get; set; }

        /// <summary>
        /// Delimiter for separating data items in CSV
        /// </summary>
        public string Delimiter { get; set; }

        /// <summary>
        /// either CR, CRLF or LF to separate rows in CSV
        /// </summary>
        public string LineSeperator { get; set; }

        /// <summary>
        /// Text identifier for alphanumeric columns in CSV
        /// </summary>
        public string TextIdentifier { get; set; }

        /// <summary>
        /// Encoding of the CSV file
        /// </summary>
        public string Encoding { get; set; }

        /// <summary>
        /// Maximum number of characters to store 
        /// </summary>
        public int MaxCharsToStore { get; set; }
    }

    /// <summary>
    /// Parameters to save results as Excel
    /// </summary>
    public class SaveResultsAsExcelRequestParams : SaveResultsRequestParams
    {
        /// <summary>
        /// Include headers of columns in Excel 
        /// </summary>
        public bool IncludeHeaders { get; set; }

        /// <summary>
        /// Freeze header row in Excel 
        /// </summary>
        public bool FreezeHeaderRow { get; set; }

        /// <summary>
        /// Bold header row in Excel 
        /// </summary>
        public bool BoldHeaderRow { get; set; }

        /// <summary>
        /// Enable auto filter on header row in Excel 
        /// </summary>
        public bool AutoFilterHeaderRow { get; set; }

        /// <summary>
        /// Auto size columns in Excel 
        /// </summary>
        public bool AutoSizeColumns { get; set; }
    }

    /// <summary>
    /// Parameters to save results as JSON
    /// </summary>
    public class SaveResultsAsJsonRequestParams: SaveResultsRequestParams
    {
        //TODO: define config for save as JSON
    }

    /// <summary>
    /// Parameters for saving results as a Markdown table
    /// </summary>
    public class SaveResultsAsMarkdownRequestParams : SaveResultsRequestParams
    {
        /// <summary>
        /// Encoding of the CSV file
        /// </summary>
        public string Encoding { get; set; }

        /// <summary>
        /// Whether to include column names as header for the table.
        /// </summary>
        public bool IncludeHeaders { get; set; }

        /// <summary>
        /// Character sequence to separate a each row in the table. Should be either CR, CRLF, or
        /// LF. If not provided, defaults to the system default line ending sequence.
        /// </summary>
        public string LineSeparator { get; set; }
    }

    /// <summary>
    /// Parameters to save results as XML
    /// </summary>
    public class SaveResultsAsXmlRequestParams: SaveResultsRequestParams
    {
        /// <summary>
        /// Formatting of the XML file
        /// </summary>
        public bool Formatted { get; set; }
        
        /// <summary>
        /// Encoding of the XML file
        /// </summary>
        public string Encoding { get; set; }
    }

}