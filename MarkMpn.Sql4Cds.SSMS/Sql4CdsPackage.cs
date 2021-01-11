using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using EnvDTE80;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using Microsoft.Xrm.Tooling.Connector;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    /// <summary>
    /// This is the class that implements the package exposed by this assembly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The minimum requirement for a class to be considered a valid package for Visual Studio
    /// is to implement the IVsPackage interface and register itself with the shell.
    /// This package uses the helper classes defined inside the Managed Package Framework (MPF)
    /// to do it: it derives from the Package class that provides the implementation of the
    /// IVsPackage interface and uses the registration attributes defined in the framework to
    /// register itself and its components with the shell. These attributes tell the pkgdef creation
    /// utility what data to put into .pkgdef file.
    /// </para>
    /// <para>
    /// To get loaded into VS, the package must be referred by &lt;Asset Type="Microsoft.VisualStudio.VsPackage" ...&gt; in .vsixmanifest file.
    /// </para>
    /// </remarks>
    [PackageRegistration(UseManagedResourcesOnly = true, AllowsBackgroundLoading = true)]
    [InstalledProductRegistration("#110", "#112", "1.0", IconResourceID = 400)] // Info on this package for Help/About
    [ProvideMenuResource("Menus.ctmenu", 1)]
    [Guid(Sql4CdsPackage.PackageGuidString)]
    [SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1650:ElementDocumentationMustBeSpelledCorrectly", Justification = "pkgdef, VS and vsixmanifest are valid VS terms")]
    [ProvideAutoLoad(UIContextGuids.NoSolution, PackageAutoLoadFlags.BackgroundLoad)]
    [ProvideOptionPage(typeof(OptionsPage), "SQL 4 CDS", "General", 0, 0, true, 0)]
    public sealed class Sql4CdsPackage : AsyncPackage
    {
        /// <summary>
        /// Sql2FetchXmlCommandPackage GUID string.
        /// </summary>
        public const string PackageGuidString = "edc4d3ea-c057-45cc-9517-a1c77b6822e4";

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql4CdsPackage"/> class.
        /// </summary>
        public Sql4CdsPackage()
        {
            // Inside this method you can place any initialization code that does not require
            // any Visual Studio service because at this point the package object is created but
            // not sited yet inside Visual Studio environment. The place to do all the other
            // initialization is the Initialize method.
        }

        /// <summary>
        /// Initialization of the package; this method is called right after the package is sited, so this is the place
        /// where you can put all the initialization code that rely on services provided by VisualStudio.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to monitor for initialization cancellation, which can occur when VS is shutting down.</param>
        /// <param name="progress">A provider for progress updates.</param>
        /// <returns>A task representing the async work of package initialization, or an already completed task if there is none. Do not return null from this method.</returns>
        protected override async Task InitializeAsync(CancellationToken cancellationToken, IProgress<ServiceProgressData> progress)
        {
            // When initialized asynchronously, the current thread may be a background thread at this point.
            // Do any initialization that requires the UI thread after switching to the UI thread.
            await this.JoinableTaskFactory.SwitchToMainThreadAsync(cancellationToken);

            // Add the command to convert SQL to FetchXML
            var dte = (DTE2)await GetServiceAsync(typeof(EnvDTE.DTE));
            await Sql2FetchXmlCommand.InitializeAsync(this, dte);
            await FetchXml2SqlCommand.InitializeAsync(this, dte);

            // Intercept query execution
            DmlExecute.Initialize(this, dte);

            CrmServiceClient.AuthOverrideHook = AuthOverrideHook.Instance;

            // Check for an updated version
            VersionChecker.Check();
        }

        public OptionsPage Settings
        {
            get
            {
                var page = (OptionsPage)GetDialogPage(typeof(OptionsPage));
                return page;
            }
        }
    }
}
