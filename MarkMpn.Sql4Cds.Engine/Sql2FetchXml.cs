using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.ServiceModel;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Converts a SQL query to the corresponding FetchXML query
    /// </summary>
    [Obsolete("Use ExecutionPlanBuilder instead")]
    public class Sql2FetchXml
    {
        class StubTableSizeCache : ITableSizeCache
        {
            public int this[string logicalName] => 1000;
        }

        class QueryExecutionOptions : IQueryExecutionOptions
        {
            private readonly Sql2FetchXml _sql2FetchXml;

            public QueryExecutionOptions(Sql2FetchXml sql2FetchXml)
            {
                _sql2FetchXml = sql2FetchXml;
            }

            public bool Cancelled => false;

            public bool BlockUpdateWithoutWhere => false;

            public bool BlockDeleteWithoutWhere => false;

            public bool UseBulkDelete => false;

            public int BatchSize => 100;

            public bool UseTDSEndpoint => _sql2FetchXml.ForceTDSEndpoint;

            public bool UseRetrieveTotalRecordCount => true;

            public int LocaleId => 1033;

            public int MaxDegreeOfParallelism => 10;

            public bool ColumnComparisonAvailable => _sql2FetchXml.ColumnComparisonAvailable;

            public bool UseLocalTimeZone => false;

            public string PrimaryDataSource => "local";

            public Guid UserId { get; set; }

            public List<JoinOperator> JoinOperatorsAvailable => new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };

            public bool BypassCustomPlugins => false;

            public bool ConfirmInsert(int count, EntityMetadata meta)
            {
                throw new NotImplementedException();
            }

            public bool ConfirmDelete(int count, EntityMetadata meta)
            {
                throw new NotImplementedException();
            }

            public bool ConfirmUpdate(int count, EntityMetadata meta)
            {
                throw new NotImplementedException();
            }

            public bool ContinueRetrieve(int count)
            {
                throw new NotImplementedException();
            }

            public void Progress(double? progress, string message)
            {
                throw new NotImplementedException();
            }

            public void RetrievingNextPage()
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Creates a new <see cref="Sql2FetchXml"/> converter
        /// </summary>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="quotedIdentifiers">Indicates if the SQL should be parsed using quoted identifiers</param>
        public Sql2FetchXml(IAttributeMetadataCache metadata, bool quotedIdentifiers)
        {
            Metadata = new MetaMetadataCache(metadata);
            QuotedIdentifiers = quotedIdentifiers;
        }

        /// <summary>
        /// Returns the metadata cache that will be used by this conversion
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// Returns or sets a value indicating if SQL will be parsed using quoted identifiers
        /// </summary>
        public bool QuotedIdentifiers { get; set; }

        /// <summary>
        /// Indicates if the CDS TDS endpoint can be used as a fallback if a query cannot be converted to FetchXML
        /// </summary>
        public bool TDSEndpointAvailable { get; set; }

        /// <summary>
        /// Indicates if the CDS TDS endpoint should be used wherever possible
        /// </summary>
        public bool ForceTDSEndpoint { get; set; }

        /// <summary>
        /// Indicates if column comparison conditions are supported
        /// </summary>
        public bool ColumnComparisonAvailable { get; set; }

        /// <summary>
        /// Parses a SQL batch and returns the queries identified in it
        /// </summary>
        /// <param name="sql">The SQL batch to parse</param>
        /// <returns>An array of queries that can be run against CDS, converted from the supplied <paramref name="sql"/></returns>
        /// <remarks>
        /// If an error is encountered when parsing the SQL, a <see cref="QueryParseException"/> is thrown.
        /// 
        /// If the SQL can be parsed correctly but contains elements that aren't supported in the conversion to FetchXML,
        /// a <see cref="NotSupportedQueryFragmentException"/> is thrown.
        /// </remarks>
        public Query[] Convert(string sql)
        {
            // Convert the SQL using the ExecutionPlanBuilder
            var planBuilder = new ExecutionPlanBuilder(Metadata, new StubTableSizeCache(), new QueryExecutionOptions(this))
            {
                QuotedIdentifiers = QuotedIdentifiers,
                TDSEndpointAvailable = TDSEndpointAvailable
            };

            var plans = planBuilder.Build(sql);

            // Convert the generated execution plans to the old Query classes
            var queries = plans.Select(plan =>
                {
                    Query query;
                    var fetchNode = FindFirstNode<FetchXmlScan>(plan);

                    if (plan is SqlNode tds)
                    {
                        query = new SelectQuery { TSql = tds.Sql };
                    }
                    else if (plan is SelectNode select)
                    {
                        query = new SelectQuery { ColumnSet = select.ColumnSet.Select(col => col.SourceColumn).Select(col => col.StartsWith($"{fetchNode?.Alias}.") ? col.Substring(fetchNode.Alias.Length + 1) : col).ToArray() };
                    }
                    else if (plan is UpdateNode update)
                    {
                        query = new UpdateQuery();
                    }
                    else if (plan is InsertNode insert)
                    {
                        if (insert.Source is ConstantScanNode)
                            query = new InsertValues();
                        else
                            query = new InsertSelect();
                    }
                    else if (plan is DeleteNode delete)
                    {
                        query = new DeleteQuery();
                    }
                    else if (plan is ExecuteAsNode executeAs)
                    {
                        query = new ImpersonateQuery(null);
                    }
                    else if (plan is RevertNode revert)
                    {
                        query = new RevertQuery();
                    }
                    else
                    {
                        throw new ApplicationException("Unexpected query plan type " + plan.GetType());
                    }

                    query.Sql = plan.Sql;
                    query.Index = plan.Index;
                    query.Length = plan.Length;
                    query.Node = plan;

                    if (query is FetchXmlQuery fetchQuery)
                    {
                        if (fetchNode != null)
                        {
                            fetchQuery.AllPages = fetchNode.AllPages;
                            fetchQuery.FetchXml = fetchNode.FetchXml;
                        }

                        if (plan is SelectNode selectNode && selectNode.Source is TryCatchNode tryCatch)
                        {
                            AddExtensions(fetchQuery, tryCatch.TrySource, fetchNode);

                            var alternativeFetchNode = FindFirstNode<FetchXmlScan>(tryCatch.CatchSource);
                            var alternativeSelectNode = new SelectNode { Source = tryCatch.CatchSource };
                            alternativeSelectNode.ColumnSet.AddRange(selectNode.ColumnSet);
                            fetchQuery.AggregateAlternative = new SelectQuery
                            {
                                ColumnSet = selectNode.ColumnSet.Select(col => col.SourceColumn).Select(col => col.StartsWith($"{fetchNode?.Alias}.") ? col.Substring(fetchNode.Alias.Length + 1) : col).ToArray(),
                                AllPages = alternativeFetchNode.AllPages,
                                FetchXml = alternativeFetchNode.FetchXml,
                                Node = alternativeSelectNode
                            };

                            AddExtensions(fetchQuery.AggregateAlternative, tryCatch.CatchSource, alternativeFetchNode);
                        }
                        else
                        {
                            var tdsSource = FindFirstNode<SqlNode>(plan);

                            if (tdsSource != null)
                                fetchQuery.TSql = tdsSource.Sql;
                            else
                                AddExtensions(fetchQuery, plan, fetchNode);
                        }
                    }

                    return query;
                })
                .ToArray();

            return queries;
        }

        private void AddExtensions(FetchXmlQuery fetchQuery, IExecutionPlanNode plan, IExecutionPlanNode fetchNode)
        {
            foreach (var source in plan.GetSources())
            {
                if (source == fetchNode)
                    continue;

                fetchQuery.Extensions.Add(new QueryExtension(source));

                AddExtensions(fetchQuery, source, fetchNode);
            }
        }

        private T FindFirstNode<T>(IExecutionPlanNode node) where T: class, IExecutionPlanNode
        {
            if (node is T fetch)
                return fetch;

            foreach (var source in node.GetSources())
            {
                fetch = FindFirstNode<T>(source);

                if (fetch != null)
                    return fetch;
            }

            return null;
        }
    }
}

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Provides post-processing of the query results to perform queries that aren't supported directly in FetchXML
    /// </summary>
    [Obsolete("Use IExecutionPlanNode instead")]
    public interface IQueryExtension
    {
        /// <summary>
        /// Transforms the query results
        /// </summary>
        /// <param name="source">The query results to transform</param>
        /// <param name="options">The query execution options to apply</param>
        /// <returns>The transformed result set</returns>
        IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options);
    }

    class QueryExtension : IQueryExtension
    {
        public QueryExtension(IExecutionPlanNode node)
        {
            Node = node;
        }

        public IExecutionPlanNode Node { get; }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            throw new NotImplementedException();
        }
    }
}

