using System.Diagnostics;
using MarkMpn.Sql4Cds.Engine;
using Microsoft;
using Microsoft.VisualStudio.Extensibility;
using Microsoft.VisualStudio.Extensibility.Commands;
using Microsoft.VisualStudio.Extensibility.DebuggerVisualizers;
using Microsoft.VisualStudio.Extensibility.Shell;
using Microsoft.VisualStudio.Extensibility.VSSdkCompatibility;
using Microsoft.VisualStudio.RpcContracts.RemoteUI;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.DebugVisualizer.DebuggerSide
{
    /// <summary>
    /// Debugger visualizer provider for <see cref="IRootExecutionPlanNode"/>.
    /// </summary>
    [VisualStudioContribution]
    internal class ExecutionPlanDebuggerVisualizerProvider : DebuggerVisualizerProvider
    {
        private const string DisplayName = "MarkMpn.Sql4Cds.DebugVisualizer.DisplayName";

        public override DebuggerVisualizerProviderConfiguration DebuggerVisualizerProviderConfiguration => new(
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SelectNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"))
        {
            VisualizerObjectSourceType = new("MarkMpn.Sql4Cds.DebugVisualizer.DebugeeSide.ExecutionPlanObjectSource, MarkMpn.Sql4Cds.DebugVisualizer.DebugeeSide")
        };

        public override async Task<IRemoteUserControl> CreateVisualizerAsync(VisualizerTarget visualizerTarget, CancellationToken cancellationToken)
        {
            var serializedPlan = await visualizerTarget.ObjectSource.RequestDataAsync<SerializedPlan>(null, cancellationToken);
            var plan = ExecutionPlanSerializer.Deserialize(serializedPlan.Plan);

            await ThreadHelper.JoinableTaskFactory.SwitchToMainThreadAsync(cancellationToken);
            var wrapper = new WpfControlWrapper(new QueryPlanUserControl(plan));
            return wrapper;
        }
    }
}
