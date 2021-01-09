using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data.SqlClient;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.Editors;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.VisualStudio.Shell;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    /// <summary>
    /// Command handler
    /// </summary>
    internal sealed class FetchXml2SqlCommand : CommandBase
    {
        private readonly IObjectExplorerService _objectExplorer;

        /// <summary>
        /// Command ID.
        /// </summary>
        public const int CommandId = 0x0200;

        /// <summary>
        /// Command menu group (command set GUID).
        /// </summary>
        public static readonly Guid CommandSet = new Guid("fd809e45-c5a9-40cc-9f78-501dd3f71817");

        /// <summary>
        /// Initializes a new instance of the <see cref="FetchXml2SqlCommand"/> class.
        /// Adds our command handlers for menu (commands must exist in the command table file)
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        /// <param name="commandService">Command service to add command to, not null.</param>
        private FetchXml2SqlCommand(Sql4CdsPackage package, OleMenuCommandService commandService, IObjectExplorerService objectExplorer, DTE2 dte) :base(package, dte)
        {
            commandService = commandService ?? throw new ArgumentNullException(nameof(commandService));
            _objectExplorer = objectExplorer;
            
            var menuCommandID = new CommandID(CommandSet, CommandId);
            var menuItem = new OleMenuCommand(this.Execute, menuCommandID);
            menuItem.BeforeQueryStatus += QueryStatus;
            commandService.AddCommand(menuItem);
        }

        private void QueryStatus(object sender, EventArgs e)
        {
            var menuItem = (OleMenuCommand)sender;

            if (ActiveDocument == null)
            {
                menuItem.Enabled = false;
                return;
            }

            _objectExplorer.GetSelectedNodes(out var size, out var nodes);
            if (size != 1)
            {
                menuItem.Enabled = false;
                return;
            }

            var node = nodes[0];

            while (node != null && node.UrnPath != "Server")
                node = node.Parent;

            if (node == null)
            {
                menuItem.Enabled = false;
                return;
            }

            if (!IsDataverse(new SqlConnectionStringBuilder(node.Connection.ConnectionString)))
            {
                menuItem.Enabled = false;
                return;
            }

            if (ActiveDocument.Language != "XML")
            {
                menuItem.Enabled = false;
                return;
            }

            menuItem.Enabled = true;
        }

        /// <summary>
        /// Gets the instance of the command.
        /// </summary>
        public static FetchXml2SqlCommand Instance
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
            // Verify the current thread is the UI thread - the call to AddCommand in FetchXml2SqlCommand's constructor requires
            // the UI thread.
            ThreadHelper.ThrowIfNotOnUIThread();

            OleMenuCommandService commandService = await package.GetServiceAsync(typeof(IMenuCommandService)) as OleMenuCommandService;
            var objectExplorer = await package.GetServiceAsync(typeof(IObjectExplorerService)) as IObjectExplorerService;
            Instance = new FetchXml2SqlCommand(package, commandService, objectExplorer, dte);
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

            _objectExplorer.GetSelectedNodes(out var size, out var nodes);
            var conStr = new SqlConnectionStringBuilder(nodes[0].Connection.ConnectionString);

            var start = ActiveDocument.StartPoint.CreateEditPoint();
            var fetch = start.GetText(ActiveDocument.EndPoint);

            _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = "FetchXML", ["Source"] = "SSMS" });

            var sql = FetchXml2Sql.Convert(ConnectCDS(conStr), GetMetadataCache(conStr), fetch, new FetchXml2SqlOptions
            {
                ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations,
                UseParametersForLiterals = true,
                UseUtcDateTimeColumns = true
            }, out var paramValues);

            ServiceCache.ScriptFactory.CreateNewBlankScript(ScriptType.Sql, ServiceCache.ScriptFactory.CurrentlyActiveWndConnectionInfo.UIConnectionInfo, null);

            var editPoint = ActiveDocument.EndPoint.CreateEditPoint();
            editPoint.Insert("/*\r\nCreated from query:\r\n\r\n");
            editPoint.Insert(fetch);
            editPoint.Insert("\r\n\r\n*/\r\n\r\n");

            foreach (var param in paramValues)
            {
                string paramType;
                var quoteValues = false;

                switch (param.Value.GetType().Name)
                {
                    case "Int32":
                        paramType = "int";
                        break;

                    case "Decimal":
                        paramType = "numeric";
                        break;

                    case "DateTime":
                        paramType = "datetime";
                        quoteValues = true;
                        break;

                    default:
                        paramType = "nvarchar(max)";
                        quoteValues = true;
                        break;
                }

                editPoint.Insert($"DECLARE {param.Key} {paramType} = ");

                if (quoteValues)
                    editPoint.Insert("'" + param.Value.ToString().Replace("'", "''") + "'\r\n");
                else
                    editPoint.Insert(param.Value.ToString() + "\r\n");
            }

            if (paramValues.Count > 0)
                editPoint.Insert("\r\n");

            editPoint.Insert(sql);
        }
    }
}
