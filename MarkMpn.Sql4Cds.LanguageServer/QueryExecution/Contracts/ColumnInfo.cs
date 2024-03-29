﻿namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ColumnInfo
    {
        /// <summary>
        /// Name of this column
        /// </summary>
        public string Name { get; set; }

        public string DataTypeName { get; set; }

        public int NumericScale { get; set; }

        public ColumnInfo()
        {
        }

        public ColumnInfo(string name, string dataTypeName, int numericScale)
        {
            Name = name;
            DataTypeName = dataTypeName;
            NumericScale = numericScale;
        }
    }
}
