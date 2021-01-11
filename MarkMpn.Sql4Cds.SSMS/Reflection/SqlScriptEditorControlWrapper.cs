using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.QueryExecution;
using Microsoft.SqlServer.Management.Smo.RegSvrEnum;

namespace MarkMpn.Sql4Cds.SSMS
{
    class SqlScriptEditorControlWrapper : ReflectionObjectBase
    {
        public SqlScriptEditorControlWrapper(object obj) : base(obj)
        {
            Results = new DisplaySQLResultsControlWrapper(GetField(obj, "m_sqlResultsControl"));
        }

        public DisplaySQLResultsControlWrapper Results { get; }

        public ITextSpan GetSelectedTextSpan()
        {
            return (ITextSpan)InvokeMethod(Target, "GetSelectedTextSpan");
        }

        public bool QuotedIdentifiers
        {
            get
            {
                var options = GetProperty(Target, "MyOptions");
                var execSettings = GetProperty(options, "ExecutionSettings");
                return (bool) GetProperty(execSettings, "SetQuotedIdentifier");
            }
        }

        public void StandardPrepareBeforeExecute()
        {
            var executing = Enum.ToObject(Type.GetType("Microsoft.SqlServer.Management.UI.VSIntegration.Editors.QEStatusBarKnownStates, SQLEditors"), 4);
            InvokeMethod(Target, "StandardPrepareBeforeExecute", executing);
        }

        public void OnExecutionStarted(object sender, EventArgs e)
        {
            InvokeMethod(Target, "OnExecutionStarted", sender == this ? Target : sender, EventArgs.Empty);
        }

        public void ToggleResultsControl(bool show)
        {
            InvokeMethod(Target, "ToggleResultsControl", show);
        }

        public void Cancelling()
        {
            var cancelingExecution = Enum.ToObject(Type.GetType("Microsoft.SqlServer.Management.UI.VSIntegration.Editors.QEStatusBarKnownStates, SQLEditors"), 11);
            InvokeMethod(Target, "OnWindowStatusTextChanged", cancelingExecution);
        }

        public void DoCancelExec()
        {
            InvokeMethod(Target, "DoCancelExec");
        }

        public string ConnectionString
        {
            get => ((IDbConnection)GetField(Target, "m_connection"))?.ConnectionString;
        }
    }
}
