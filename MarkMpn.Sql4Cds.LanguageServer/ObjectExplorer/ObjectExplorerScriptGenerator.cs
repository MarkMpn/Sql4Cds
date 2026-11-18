using System;
using System.Linq;
using MarkMpn.Sql4Cds.Engine;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer
{
    internal static class ObjectExplorerScriptGenerator
    {
        public static string Generate(DataSource dataSource, ColumnOrdering columnOrdering, string nodeType, string schema, string name, string action)
        {
            switch (action)
            {
                case "SELECT":
                    if (nodeType == "Table")
                        return ScriptGenerator.GenerateSelectQuery(schema, dataSource.Metadata[name], null, dataSource, columnOrdering);
                    if (nodeType == "TableValuedFunction")
                        return ScriptGenerator.GenerateSelectQuery(GetMessage(dataSource, name, true), dataSource, columnOrdering);
                    break;
                case "SELECT_TOP_1000":
                    if (nodeType == "Table")
                        return ScriptGenerator.GenerateSelectQuery(schema, dataSource.Metadata[name], 1000, dataSource, columnOrdering);
                    break;
                case "INSERT":
                    if (nodeType == "Table")
                        return ScriptGenerator.GenerateInsertQuery(schema, dataSource.Metadata[name], dataSource, columnOrdering);
                    break;
                case "UPDATE":
                    if (nodeType == "Table")
                        return ScriptGenerator.GenerateUpdateQuery(schema, dataSource.Metadata[name], dataSource, columnOrdering);
                    break;
                case "DELETE":
                    if (nodeType == "Table")
                        return ScriptGenerator.GenerateDeleteQuery(schema, name);
                    break;
                case "EXECUTE":
                    if (nodeType == "StoredProcedure")
                        return ScriptGenerator.GenerateExecuteQuery(GetMessage(dataSource, name, false), dataSource, columnOrdering);
                    break;
            }

            throw new ArgumentException($"Cannot generate a {action} script for a {nodeType} object.");
        }

        private static Message GetMessage(DataSource dataSource, string name, bool tableValuedFunction)
        {
            var message = dataSource.MessageCache.GetAllMessages(false).SingleOrDefault(message => message.Name == name);
            if (message == null || (tableValuedFunction ? !message.IsValidAsTableValuedFunction() : !message.IsValidAsStoredProcedure()))
                throw new InvalidOperationException($"The {name} message is no longer available.");
            return message;
        }
    }
}
