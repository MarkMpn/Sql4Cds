using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    /// <summary>
    /// Command handler
    /// </summary>
    internal sealed class Sql2MCommand : CommandBase
    {
        /// <summary>
        /// Command ID.
        /// </summary>
        public const int CommandId = 0x0300;

        /// <summary>
        /// Command menu group (command set GUID).
        /// </summary>
        public static readonly Guid CommandSet = new Guid("fd809e45-c5a9-40cc-9f78-501dd3f71817");

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql2MCommand"/> class.
        /// Adds our command handlers for menu (commands must exist in the command table file)
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        /// <param name="commandService">Command service to add command to, not null.</param>
        private Sql2MCommand(Sql4CdsPackage package, OleMenuCommandService commandService, DTE2 dte) : base(package, dte)
        {
            commandService = commandService ?? throw new ArgumentNullException(nameof(commandService));

            var menuCommandID = new CommandID(CommandSet, CommandId);
            var menuItem = new OleMenuCommand(this.Execute, menuCommandID);
            menuItem.BeforeQueryStatus += QueryStatus;
            commandService.AddCommand(menuItem);
        }

        private void QueryStatus(object sender, EventArgs e)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var menuItem = (OleMenuCommand)sender;

            if (ActiveDocument == null || ActiveDocument.Language != "SQL" || !IsDataverse())
            {
                menuItem.Enabled = false;
                return;
            }

            menuItem.Enabled = true;
        }

        /// <summary>
        /// Gets the instance of the command.
        /// </summary>
        public static Sql2MCommand Instance
        {
            get;
            private set;
        }

        /// <summary>
        /// Initializes the singleton instance of the command.
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        public static async Task InitializeAsync(Sql4CdsPackage package, DTE2 dte)
        {
            OleMenuCommandService commandService = await package.GetServiceAsync((typeof(IMenuCommandService))) as OleMenuCommandService;
            Instance = new Sql2MCommand(package, commandService, dte);
        }

        /// <summary>
        /// This function is the callback used to execute the command when the menu item is clicked.
        /// See the constructor to see how the menu item is associated with this function using
        /// OleMenuCommandService service and MenuCommand class.
        /// </summary>
        /// <param name="sender">Event sender.</param>
        /// <param name="e">Event args.</param>
        private void Execute(object sender, EventArgs e)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            try
            {
                var scriptFactory = new ScriptFactoryWrapper(ServiceCache.ScriptFactory);
                var sqlScriptEditorControl = scriptFactory.GetCurrentlyActiveFrameDocView(ServiceCache.VSMonitorSelection, false, out _);

                var constr = GetConnectionInfo(true);
                var server = constr.DataSource.Split(',')[0];

                _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = "M", ["Source"] = "SSMS" });

                var sql = GetQuery();
                var m = $@"/*
Query converted to M format by SQL 4 CDS
To use in Power BI:
1. Click New Source
2. Click Blank Query
3. Click Advanced Editor
4. Copy & paste in this query
*/

let
    Source = CommonDataService.Database(""{server}""),
    DataverseSQL = Value.NativeQuery(Source, ""{sql.Replace("\"", "\"\"").Replace("\r\n", " ").Trim()}"", null, [EnableFolding=true])
in
    DataverseSQL";

                var window = Dte.ItemOperations.NewFile("General\\Text File");

                var editPoint = ActiveDocument.EndPoint.CreateEditPoint();
                editPoint.Insert(m);
            }
            catch (Exception ex)
            {
                VsShellUtilities.LogError("SQL 4 CDS", ex.ToString());
            }
        }

        private IEnumerable<IExecutionPlanNode> GetAllNodes(IExecutionPlanNode node)
        {
            foreach (var source in node.GetSources())
            {
                yield return source;

                foreach (var subSource in GetAllNodes(source))
                    yield return subSource;
            }
        }
    }
}
