using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.Editors;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;

namespace MarkMpn.Sql4Cds.SSMS
{
    class ScriptFactoryWrapper : ReflectionObjectBase
    {
        public ScriptFactoryWrapper(IScriptFactory obj) : base(obj)
        {
        }

        public SqlScriptEditorControlWrapper GetCurrentlyActiveFrameDocView(
#if SSMS21
            Microsoft.VisualStudio.Shell.Interop.IVsMonitorSelection pMonSel,
#else
            Microsoft.SqlServer.Management.UI.VSIntegration.IVsMonitorSelection pMonSel,
#endif
            bool checkDocView, out IVsWindowFrame frame)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var parameters = new object[] { pMonSel, checkDocView, null };
            var obj = InvokeMethod(Target, "GetCurrentlyActiveFrameDocView", parameters);
            frame = (IVsWindowFrame) parameters[2];

            if (!(obj is SqlScriptEditorControl ctrl))
                return null;

            return new SqlScriptEditorControlWrapper(ctrl);
        }
    }
}
