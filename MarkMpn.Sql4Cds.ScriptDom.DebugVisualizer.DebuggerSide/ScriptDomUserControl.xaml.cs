using ColorCode;
using ColorCode.Styling;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.Extensibility.DebuggerVisualizers;
using Microsoft.VisualStudio.PlatformUI;
using Microsoft.Web.WebView2.Core;
using Newtonsoft.Json;
using System.Drawing;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using MessageBox = System.Windows.MessageBox;

namespace MarkMpn.Sql4Cds.ScriptDom.DebugVisualizer.DebuggerSide
{
    // https://github.com/Giorgi/EFCore.Visualizer/blob/main/src/EFCore.Visualizer/QueryPlanUserControl.xaml.cs
    public partial class ScriptDomUserControl : System.Windows.Controls.UserControl
    {
        private string? filePath;
        private readonly VisualizerTarget visualizerTarget;
        private Color backgroundColor = VSColorTheme.GetThemedColor(ThemedDialogColors.WindowPanelBrushKey);
        private static readonly string AssemblyLocation = Path.GetDirectoryName(typeof(ScriptDomUserControl).Assembly.Location);

        public ScriptDomUserControl(VisualizerTarget visualizerTarget)
        {
            this.visualizerTarget = visualizerTarget;
            InitializeComponent();

            Unloaded += ScriptDomUserControlUnloaded;
        }

        private void ScriptDomUserControlUnloaded(object sender, RoutedEventArgs e)
        {
            SafeDeleteFile(filePath);

            Unloaded -= ScriptDomUserControlUnloaded;
        }

#pragma warning disable VSTHRD100 // Avoid async void methods
        protected override async void OnInitialized(EventArgs e)
#pragma warning restore VSTHRD100 // Avoid async void methods
        {
            SafeDeleteFile(filePath);

            try
            {
                base.OnInitialized(e);

                var environment = await CoreWebView2Environment.CreateAsync(userDataFolder: Path.Combine(AssemblyLocation, "WVData"));
                await webView.EnsureCoreWebView2Async(environment);

                webView.CoreWebView2.Profile.PreferredColorScheme = IsBackgroundDarkColor(backgroundColor) ? CoreWebView2PreferredColorScheme.Dark : CoreWebView2PreferredColorScheme.Light;
#if !DEBUG
                webView.CoreWebView2.Settings.AreBrowserAcceleratorKeysEnabled = false;
                webView.CoreWebView2.Settings.AreDefaultContextMenusEnabled = false;
#endif
                var fragment = await GetFragmentAsync();
                new Sql160ScriptGenerator().GenerateScript(fragment, out var sql);
                var formatter = new HtmlFormatter(IsBackgroundDarkColor(backgroundColor) ? StyleDictionary.DefaultDark : StyleDictionary.DefaultLight);
                var html = formatter.GetHtmlString(sql, Languages.Sql);
                filePath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Path.GetRandomFileName(), "html"));
                File.WriteAllText(filePath, html);

                treeView.Items.Clear();
                treeView.Items.Add(CreateTreeViewItem(fragment, null));
            }
            catch (Exception ex)
            {
                MessageBox.Show("Cannot retrieve script: " + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                if (!string.IsNullOrEmpty(filePath))
                {
                    webView.CoreWebView2.Navigate(filePath);
                }
            }
        }

        private TreeViewItem CreateTreeViewItem(TSqlFragment fragment, string prefix)
        {
            var item = new TreeViewItem { Header = (prefix == null ? null : (prefix + " - ")) + fragment.GetType().Name };

            foreach (var prop in fragment.GetType().GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0))
            {
                var child = prop.GetValue(fragment);

                if (child == null)
                    continue;

                if (child is TSqlFragment childFragment)
                {
                    item.Items.Add(CreateTreeViewItem((TSqlFragment)child, prop.Name));
                }
                else if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(IList<>) && typeof(TSqlFragment).IsAssignableFrom(prop.PropertyType.GetGenericArguments()[0]))
                {
                    var i = 0;

                    foreach (TSqlFragment childItemFragment in (System.Collections.IEnumerable)child)
                    {
                        item.Items.Add(CreateTreeViewItem(childItemFragment, $"{prop.Name}[{i}]"));
                        i++;
                    }
                }
            }

            item.IsExpanded = true;
            item.MouseEnter += HighlightFragment;
            item.MouseLeave += UnHighlightFragment;
            item.Tag = fragment;
            return item;
        }

        private void HighlightFragment(object sender, MouseEventArgs e)
        {
            var item = (TreeViewItem)sender;
            var fragment = (TSqlFragment)item.Tag;
        }

        private void UnHighlightFragment(object sender, MouseEventArgs e)
        {
            var item = (TreeViewItem)sender;
            var fragment = (TSqlFragment)item.Tag;
        }

        private async Task<TSqlFragment> GetFragmentAsync()
        {
            var serializedFragment = await visualizerTarget.ObjectSource.RequestDataAsync<SerializedFragment>(null, CancellationToken.None);
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            };
            return JsonConvert.DeserializeObject<TSqlFragment>(serializedFragment.Fragment, settings);
        }

        private static void SafeDeleteFile(string? path)
        {
            if (string.IsNullOrWhiteSpace(path)) return;

            try
            {
                File.Delete(path);
            }
            catch
            {
                // Ignore
            }
        }

        private static bool IsBackgroundDarkColor(Color color) => color.R * 0.2126 + color.G * 0.7152 + color.B * 0.0722 < 255 / 2.0;

        private void WebViewNavigationCompleted(object sender, CoreWebView2NavigationCompletedEventArgs e)
        {
            _ = webView.CoreWebView2.ExecuteScriptAsync($"document.querySelector(':root').style.setProperty('--bg-color', 'RGB({backgroundColor.R}, {backgroundColor.G}, {backgroundColor.B})');");
        }
    }
}
