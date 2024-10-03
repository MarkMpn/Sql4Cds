using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides access to the context in which a node will be executed
    /// </summary>
    class NodeCompilationContext
    {
        private readonly NodeCompilationContext _parentContext;
        private int _expressionCounter;

        /// <summary>
        /// Creates a new <see cref="NodeCompilationContext"/>
        /// </summary>
        /// <param name="dataSources">The data sources that are available to the query</param>
        /// <param name="options">The options that the query will be executed with</param>
        /// <param name="parameterTypes">The names and types of the parameters that are available to the query</param>
        /// <param name="log">A callback function to log messages</param>
        public NodeCompilationContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes,
            Action<Sql4CdsError> log)
        {
            DataSources = dataSources;
            Options = options;
            ParameterTypes = parameterTypes;
            GlobalCalculations = new NestedLoopNode
            {
                LeftSource = new ComputeScalarNode
                {
                    Source = new ConstantScanNode
                    {
                        Values =
                        {
                            new Dictionary<string, ScalarExpression>()
                        }
                    }
                },
                OuterReferences = new Dictionary<string, string>()
            };
            Log = log ?? (msg => { });
            DateFormat = DateFormat.mdy;
        }

        /// <summary>
        /// Creates a new <see cref="NodeCompilationContext"/> as a child of another context
        /// </summary>
        /// <param name="parentContext">The parent context that this context is being created from</param>
        /// <param name="parameterTypes">The names and types of the parameters that are available to this section of the query</param>
        public NodeCompilationContext(
            NodeCompilationContext parentContext,
            IDictionary<string, DataTypeReference> parameterTypes)
        {
            DataSources = parentContext.DataSources;
            Options = parentContext.Options;
            ParameterTypes = parameterTypes;
            GlobalCalculations = parentContext.GlobalCalculations;
            Log = parentContext.Log;
            DateFormat = parentContext.DateFormat;
            _parentContext = parentContext;
        }

        /// <summary>
        /// Returns the data sources that are available to the query
        /// </summary>
        public IDictionary<string, DataSource> DataSources { get; }

        /// <summary>
        /// Returns the options that the query will be executed with
        /// </summary>
        public IQueryExecutionOptions Options { get; }

        /// <summary>
        /// Returns the names and types of the parameters that are available to the query
        /// </summary>
        public IDictionary<string, DataTypeReference> ParameterTypes { get; }

        /// <summary>
        /// Returns the details of the primary data source
        /// </summary>
        public DataSource PrimaryDataSource => DataSources[Options.PrimaryDataSource];

        /// <summary>
        /// Returns a <see cref="NestedLoopNode"/> which can be used to calculate global values to be injected into other nodes
        /// </summary>
        public NestedLoopNode GlobalCalculations { get; }

        /// <summary>
        /// A callback function to log messages
        /// </summary>
        public Action<Sql4CdsError> Log { get; }

        /// <summary>
        /// Returns or sets the current SET DATEFORMAT option
        /// </summary>
        public DateFormat DateFormat { get; set; }

        /// <summary>
        /// Generates a unique name for an expression
        /// </summary>
        /// <returns>The name to use for the expression</returns>
        public string GetExpressionName()
        {
            if (_parentContext != null)
                return _parentContext.GetExpressionName();

            return $"Expr{++_expressionCounter}";
        }

        internal void ResetGlobalCalculations()
        {
            GlobalCalculations.OuterReferences.Clear();
            ((ComputeScalarNode)GlobalCalculations.LeftSource).Columns.Clear();
        }

        internal IDataExecutionPlanNodeInternal InsertGlobalCalculations(IRootExecutionPlanNodeInternal rootNode, IDataExecutionPlanNodeInternal source)
        {
            if (GlobalCalculations.OuterReferences.Count == 0)
                return source;

            var clone = (NestedLoopNode)GlobalCalculations.Clone();
            clone.RightSource = source;
            source.Parent = clone;
            clone.Parent = rootNode;

            return clone;
        }
    }

    /// <summary>
    /// Provides access to the context in which a node is being executed
    /// </summary>
    class NodeExecutionContext : NodeCompilationContext
    {
        /// <summary>
        /// Creates a new <see cref="NodeExecutionContext"/>
        /// </summary>
        /// <param name="dataSources">The data sources that are available to the query</param>
        /// <param name="options">The options that the query is being executed with</param>
        /// <param name="parameterTypes">The names and types of the parameters that are available to the query</param>
        /// <param name="parameterValues">The current value of each parameter</param>
        /// <param name="log">A callback function to log messages</param>
        public NodeExecutionContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes,
            IDictionary<string, INullable> parameterValues,
            Action<Sql4CdsError> log)
            : base(dataSources, options, parameterTypes, log)
        {
            ParameterValues = parameterValues;
        }

        /// <summary>
        /// Creates a new <see cref="NodeExecutionContext"/> based on a <see cref="NodeCompilationContext"/>
        /// </summary>
        /// <param name="parentContext">The <see cref="NodeCompilationContext"/> to inherit settings from</param>
        /// <param name="parameterValues">The values to use for any parameters</param>
        public NodeExecutionContext(
            NodeCompilationContext parentContext,
            IDictionary<string, INullable> parameterValues)
            : base(parentContext, parentContext.ParameterTypes)
        {
            ParameterValues = parameterValues;
        }

        /// <summary>
        /// Creates a new <see cref="NodeExecutionContext"/> based on another context but with additional parameters for a subquery
        /// </summary>
        /// <param name="parentContext">The <see cref="NodeExecutionContext"/> to inherit settings from</param>
        /// <param name="parameterTypes">The names and types of the parameters that are available to the subquery</param>
        /// <param name="parameterValues">The current value of each parameter</param>
        public NodeExecutionContext(
            NodeExecutionContext parentContext,
            IDictionary<string, DataTypeReference> parameterTypes,
            IDictionary<string, INullable> parameterValues)
            : base(parentContext, parameterTypes)
        {
            ParameterValues = parameterValues;
            DateFormat = parentContext.DateFormat;
        }

        /// <summary>
        /// Returns the current value of each parameter
        /// </summary>
        public IDictionary<string, INullable> ParameterValues { get; }

        /// <summary>
        /// Returns or sets the current error
        /// </summary>
        public Sql4CdsError Error { get; set; }
    }

    /// <summary>
    /// Provides access to the context in which an expression will be evaluated
    /// </summary>
    class ExpressionCompilationContext : NodeCompilationContext
    {
        /// <summary>
        /// Creates a new <see cref="ExpressionCompilationContext"/>
        /// </summary>
        /// <param name="dataSources">The data sources that are available to the query</param>
        /// <param name="options">The options that the query is being executed with</param>
        /// <param name="parameterTypes">The names and types of the parameters that are available to the query</param>
        /// <param name="schema">The schema of data which is available to the expression</param>
        /// <param name="nonAggregateSchema">The schema of data prior to aggregation</param>
        public ExpressionCompilationContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes,
            INodeSchema schema,
            INodeSchema nonAggregateSchema)
            : base(dataSources, options, parameterTypes, null)
        {
            Schema = schema;
            NonAggregateSchema = nonAggregateSchema;
        }

        /// <summary>
        /// Creates a new <see cref="ExpressionCompilationContext"/> based on a <see cref="NodeCompilationContext"/>
        /// </summary>
        /// <param name="nodeContext">The <see cref="NodeCompilationContext"/> to copy options from</param>
        /// <param name="schema">The schema of data which is available to the expression</param>
        /// <param name="nonAggregateSchema">The schema of data prior to aggregation</param>
        public ExpressionCompilationContext(
            NodeCompilationContext nodeContext,
            INodeSchema schema,
            INodeSchema nonAggregateSchema)
            : base(nodeContext.DataSources, nodeContext.Options, nodeContext.ParameterTypes, nodeContext.Log)
        {
            Schema = schema;
            NonAggregateSchema = nonAggregateSchema;
        }

        /// <summary>
        /// Returns the schema of data which is available to the expression
        /// </summary>
        public INodeSchema Schema { get; }

        /// <summary>
        /// Returns the schema of data prior to aggregation
        /// </summary>
        /// <remarks>
        /// Used to provide more helpful error messages when a non-aggregated field is incorrectly referenced after aggregation
        /// </remarks>
        public INodeSchema NonAggregateSchema { get; }
    }

    /// <summary>
    /// Provides access to the context in which an expression is being evaluated
    /// </summary>
    class ExpressionExecutionContext : NodeExecutionContext
    {
        /// <summary>
        /// Creates a new <see cref="ExpressionExecutionContext"/>
        /// </summary>
        /// <param name="dataSources">The data sources that are available to the query</param>
        /// <param name="options">The options that the query is being executed with</param>
        /// <param name="entity">The values for the current row the expression is being evaluated for</param>
        /// <param name="parameterValues">The current value of each parameter</param>
        public ExpressionExecutionContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes,
            IDictionary<string, INullable> parameterValues,
            Action<Sql4CdsError> log,
            Entity entity)
            : base(dataSources, options, parameterTypes, parameterValues, log)
        {
            Entity = entity;
        }

        /// <summary>
        /// Creates a new <see cref="ExpressionExecutionContext"/> based on a <see cref="NodeExecutionContext"/>
        /// </summary>
        /// <param name="nodeContext">The <see cref="NodeExecutionContext"/> to copy options from</param>
        /// <remarks>
        /// The returned instance is designed to be reused for multiple rows within the same node. The
        /// <see cref="Entity"/> property will initially be <c>null</c> - set it to the <see cref="Microsoft.Xrm.Sdk.Entity"/>
        /// representing each row as it is processed.
        /// </remarks>
        public ExpressionExecutionContext(NodeExecutionContext nodeContext)
            : base(nodeContext.DataSources, nodeContext.Options, nodeContext.ParameterTypes, nodeContext.ParameterValues, nodeContext.Log)
        {
            Entity = null;
            Error = nodeContext.Error;
        }

        /// <summary>
        /// Creates a new <see cref="ExpressionExecutionContext"/> based on a <see cref="ExpressionCompilationContext"/>
        /// </summary>
        /// <param name="compilationContext">The <see cref="ExpressionCompilationContext"/> to copy options from</param>
        /// <remarks>
        /// As no parameter values are specified in this constructor, this is only suitable for use when the expression to
        /// be evaluated does not consume any parameters.
        /// 
        /// The returned instance is designed to be reused for multiple rows within the same node. The
        /// <see cref="Entity"/> property will initially be <c>null</c> - set it to the <see cref="Microsoft.Xrm.Sdk.Entity"/>
        /// representing each row as it is processed.
        /// </remarks>
        public ExpressionExecutionContext(ExpressionCompilationContext compilationContext)
            : base(compilationContext.DataSources, compilationContext.Options, compilationContext.ParameterTypes, null, null)
        {
            Entity = null;
        }

        /// <summary>
        /// Returns the values for the current row the expression is being evaluated for
        /// </summary>
        public Entity Entity { get; set; }
    }
}
