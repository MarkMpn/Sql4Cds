using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    class DmlExecute : CommandBase
    {
        public DmlExecute(DTE2 dte, IObjectExplorerService objExp) : base(dte, objExp)
        {
            var execute = dte.Commands.Item("Query.Execute");
            QueryExecuteEvent = dte.Events.CommandEvents[execute.Guid, execute.ID];
            QueryExecuteEvent.BeforeExecute += OnExecuteQuery;
        }

        public CommandEvents QueryExecuteEvent { get; private set; }

        /// <summary>
        /// Gets the instance of the command.
        /// </summary>
        public static DmlExecute Instance
        {
            get;
            private set;
        }

        public static void Initialize(AsyncPackage package, DTE2 dte, IObjectExplorerService objExp)
        {
            Instance = new DmlExecute(dte, objExp);
        }

        private void OnExecuteQuery(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            if (ActiveDocument == null)
                return;

            if (!IsDataverse())
                return;

            // We are running a query against the Dataverse endpoint, so check if there are any DML statements in the query
            var sql = GetQuery();
            var sql2FetchXml = new Sql2FetchXml(GetMetadataCache(), false);
            sql2FetchXml.ColumnComparisonAvailable = true;
            sql2FetchXml.TSqlEndpointAvailable = true;
            var queries = sql2FetchXml.Convert(sql);

            var hasSelect = queries.OfType<SelectQuery>().Count();
            var hasDml = queries.Length - hasSelect;

            if (hasSelect > 0 && hasDml > 0)
            {
                // Can't mix SELECT and DML queries as we can't show results in the grid and SSMS can't execute the DML queries
                CancelDefault = true;
                return;
            }

            if (hasSelect > 0)
                return;

            // We need to execute the DML statements directly
            CancelDefault = true;

            foreach (var query in queries)
                query.Execute(ConnectCDS(), GetMetadataCache(), new QueryExecutionOptions());
        }
    }
}
