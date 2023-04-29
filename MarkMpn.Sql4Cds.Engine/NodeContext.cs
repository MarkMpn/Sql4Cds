using System;
using System.Collections.Generic;
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
        public NodeCompilationContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes)
        {
            DataSources = dataSources;
            Options = options;
            ParameterTypes = parameterTypes;
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
        /// Generates a unique name for an expression
        /// </summary>
        /// <returns>The name to use for the expression</returns>
        public string GetExpressionName()
        {
            if (_parentContext != null)
                return _parentContext.GetExpressionName();

            return $"Expr{++_expressionCounter}";
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
        public NodeExecutionContext(
            IDictionary<string, DataSource> dataSources,
            IQueryExecutionOptions options,
            IDictionary<string, DataTypeReference> parameterTypes,
            IDictionary<string, object> parameterValues)
            : base(dataSources, options, parameterTypes)
        {
            ParameterValues = parameterValues;
        }

        /// <summary>
        /// Returns the current value of each parameter
        /// </summary>
        public IDictionary<string, object> ParameterValues { get; }
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
            : base(dataSources, options, parameterTypes)
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
            : base(nodeContext.DataSources, nodeContext.Options, nodeContext.ParameterTypes)
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
            IDictionary<string, object> parameterValues,
            Entity entity)
            : base(dataSources, options, parameterTypes, parameterValues)
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
            : base(nodeContext.DataSources, nodeContext.Options, nodeContext.ParameterTypes, nodeContext.ParameterValues)
        {
            Entity = null;
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
            : base(compilationContext.DataSources, compilationContext.Options, compilationContext.ParameterTypes, null)
        {
            Entity = null;
        }

        /// <summary>
        /// Returns the values for the current row the expression is being evaluated for
        /// </summary>
        public Entity Entity { get; set; }
    }
}
