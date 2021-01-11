using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    /// <summary>
    /// Command handler
    /// </summary>
    internal sealed class Sql2FetchXmlCommand : CommandBase
    {
        /// <summary>
        /// Command ID.
        /// </summary>
        public const int CommandId = 0x0100;

        /// <summary>
        /// Command menu group (command set GUID).
        /// </summary>
        public static readonly Guid CommandSet = new Guid("fd809e45-c5a9-40cc-9f78-501dd3f71817");

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql2FetchXmlCommand"/> class.
        /// Adds our command handlers for menu (commands must exist in the command table file)
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        /// <param name="commandService">Command service to add command to, not null.</param>
        private Sql2FetchXmlCommand(Sql4CdsPackage package, OleMenuCommandService commandService, DTE2 dte) : base(package, dte)
        {
            commandService = commandService ?? throw new ArgumentNullException(nameof(commandService));

            var menuCommandID = new CommandID(CommandSet, CommandId);
            var menuItem = new OleMenuCommand(this.Execute, menuCommandID);
            menuItem.BeforeQueryStatus += QueryStatus;
            commandService.AddCommand(menuItem);
        }

        private void QueryStatus(object sender, EventArgs e)
        {
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
        public static Sql2FetchXmlCommand Instance
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
            // Verify the current thread is the UI thread - the call to AddCommand in Sql2FetchXmlCommand's constructor requires
            // the UI thread.
            ThreadHelper.ThrowIfNotOnUIThread();
            
            OleMenuCommandService commandService = await package.GetServiceAsync((typeof(IMenuCommandService))) as OleMenuCommandService;
            Instance = new Sql2FetchXmlCommand(package, commandService, dte);
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

            var sql = GetQuery();

            var sql2FetchXml = new Sql2FetchXml(GetMetadataCache(), false);
            sql2FetchXml.ColumnComparisonAvailable = true;

            try
            {
                var queries = sql2FetchXml.Convert(sql);

                foreach (var query in queries.OfType<FetchXmlQuery>())
                {
                    _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "SSMS" });

                    var window = Dte.ItemOperations.NewFile("General\\XML File");

                    var editPoint = ActiveDocument.EndPoint.CreateEditPoint();
                    editPoint.Insert("<!--\r\nCreated from query:\r\n\r\n");
                    editPoint.Insert(query.Sql);

                    if (query.Extensions.Count > 0)
                    {
                        editPoint.Insert("‼ WARNING ‼\r\n");
                        editPoint.Insert("This query requires additional processing. This FetchXML gives the required data, but needs additional processing to format it in the same way as returned by the TDS Endpoint or SQL 4 CDS.\r\n\r\n");
                        editPoint.Insert("Learn more at https://markcarrington.dev/sql-4-cds/additional-processing/\r\n\r\n");
                    }

                    if (query.DistinctWithoutSort)
                    {
                        editPoint.Insert("‼ WARNING ‼\r\n");
                        editPoint.Insert("This DISTINCT query does not have a sort order applied. Unexpected results may be returned when the results are split over multiple pages. Add a sort order to retrieve the correct results.\r\n");
                        editPoint.Insert("Learn more at https://docs.microsoft.com/powerapps/developer/common-data-service/org-service/paging-behaviors-and-ordering#ordering-with-a-paging-cookie\r\n\r\n");
                    }

                    editPoint.Insert("\r\n\r\n-->\r\n\r\n");
                    editPoint.Insert(query.FetchXmlString);
                }
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                VsShellUtilities.ShowMessageBox(Package, "The query could not be converted to FetchXML: " + ex.Message, "Query Not Supported", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
            }
            catch (QueryParseException ex)
            {
                VsShellUtilities.ShowMessageBox(Package, "The query could not be parsed: " + ex.Message, "Query Parsing Error", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
            }
        }
    }
}
