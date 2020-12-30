using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.Editors;
using Microsoft.VisualStudio.Shell.Interop;

namespace MarkMpn.Sql4Cds.SSMS
{
    class ScriptFactoryWrapper : ReflectionObjectBase
    {
        public ScriptFactoryWrapper(IScriptFactory obj) : base(obj)
        {
        }

        public SqlScriptEditorControlWrapper GetCurrentlyActiveFrameDocView(Microsoft.SqlServer.Management.UI.VSIntegration.IVsMonitorSelection pMonSel, bool checkDocView, out IVsWindowFrame frame)
        {
            var parameters = new object[] { pMonSel, checkDocView, null };
            var obj = InvokeMethod(Target, "GetCurrentlyActiveFrameDocView", parameters);
            frame = (IVsWindowFrame) parameters[2];

            if (obj == null)
                return null;

            return new SqlScriptEditorControlWrapper(obj);
        }
    }
}
