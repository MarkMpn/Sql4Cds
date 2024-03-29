﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.QueryExecution;
using Microsoft.SqlServer.Management.Smo.RegSvrEnum;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    class SqlScriptEditorControlWrapper : ReflectionObjectBase
    {
        private static readonly Type QEStatusBarKnownStates;

        static SqlScriptEditorControlWrapper()
        {
            QEStatusBarKnownStates = GetType("Microsoft.SqlServer.Management.UI.VSIntegration.Editors.QEStatusBarKnownStates, SQLEditors");
        }

        public SqlScriptEditorControlWrapper(Microsoft.SqlServer.Management.UI.VSIntegration.Editors.SqlScriptEditorControl obj) : base(obj)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            Results = new DisplaySQLResultsControlWrapper(GetField(obj, "m_sqlResultsControl"));
            ServiceProvider = new ServiceProvider((Microsoft.VisualStudio.OLE.Interop.IServiceProvider)GetField(obj, "m_serviceProvider"));
        }

        public DisplaySQLResultsControlWrapper Results { get; }

        public IServiceProvider ServiceProvider { get; }

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

        public bool IsWithShowPlan => (bool)GetProperty(Target, nameof(IsWithShowPlan));

        public void StandardPrepareBeforeExecute()
        {
            var executing = Enum.ToObject(QEStatusBarKnownStates, 4);
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
            var cancelingExecution = Enum.ToObject(QEStatusBarKnownStates, 11);
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
