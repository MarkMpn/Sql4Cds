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
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.AdaptiveIndexSpoolNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.AliasNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.AssertNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.AssignVariablesNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BulkDeleteJobNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.CloseCursorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ComputeScalarNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ConcatenateNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ConditionalNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ConstantScanNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ContinueBreakNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.CreateTableNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DeallocateCursorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DeclareVariablesNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DeleteNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DistinctNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DropTableNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ExecuteAsNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ExecuteMesageNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.FetchCursorIntoNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.FetchCursorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.FetchXmlScan, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.FilterNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.GlobalOptionSetQueryNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.GotoLabelNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.GoToNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.HashJoinNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.HashMatchAggregateNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.IndexSpoolNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.InsertNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.MergeJoinNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.MetadataQueryNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.NestedLoopNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.OffsetFetchNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.OpenCursorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.OpenJsonNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.PartitionedAggregateNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.PrintNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.RaiseErrorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.RetrieveTotalRecordCountNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.RevertNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SegmentNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SelectNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SequenceProjectNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SetDateFormatNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SortNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SqlNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.StaticCursorNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.StreamAggregateNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.StringSplitNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SystemFunctionNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.TableScanNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.TableSpoolNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ThrowNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.TopNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BeginTryNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.EndTryNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BeginCatchNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.EndCatchNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.UnparsedConditionalNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.UnparsedGoToNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.UnparsedStatementNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.UpdateNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.WaitForNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.WindowSpoolNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.ExecutionPlan.XmlWriterNode, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"),
            new VisualizerTargetType($"%{DisplayName}%", "MarkMpn.Sql4Cds.Engine.Sql4CdsCommand, MarkMpn.Sql4Cds.Engine, Version=0.0.0.0, Culture=neutral"))
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
