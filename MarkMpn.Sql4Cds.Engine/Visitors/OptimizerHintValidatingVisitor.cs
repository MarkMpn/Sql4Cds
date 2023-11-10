using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks all optimizer hint names are valid. All T-SQL hint names are valid, plus some bespoke ones.
    /// </summary>
    class OptimizerHintValidatingVisitor : TSqlFragmentVisitor
    {
        private static readonly HashSet<string> _tsqlQueryHints = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS",
            "ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES",
            "ASSUME_FULL_INDEPENDENCE_FOR_FILTER_ESTIMATES",
            "ASSUME_PARTIAL_CORRELATION_FOR_FILTER_ESTIMATES",
            "DISABLE_BATCH_MODE_ADAPTIVE_JOINS",
            "DISABLE_BATCH_MODE_MEMORY_GRANT_FEEDBACK",
            "DISABLE_DEFERRED_COMPILATION_TV",
            "DISABLE_INTERLEAVED_EXECUTION_TVF",
            "DISABLE_OPTIMIZED_NESTED_LOOP",
            "DISABLE_OPTIMIZER_ROWGOAL",
            "DISABLE_PARAMETER_SNIFFING",
            "DISABLE_ROW_MODE_MEMORY_GRANT_FEEDBACK",
            "DISABLE_TSQL_SCALAR_UDF_INLINING",
            "DISALLOW_BATCH_MODE",
            "ENABLE_HIST_AMENDMENT_FOR_ASC_KEYS",
            "ENABLE_QUERY_OPTIMIZER_HOTFIXES",
            "FORCE_DEFAULT_CARDINALITY_ESTIMATION",
            "FORCE_LEGACY_CARDINALITY_ESTIMATION",
            "QUERY_PLAN_PROFILE",
        };

        private static readonly HashSet<string> _sql4cdsQueryHints = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // Custom hints to force the query to run in SQL 4 CDS rather than TDS Endpoint
            "FORCE_SQL4CDS",
            "FORCE_SQL_4_CDS",

            // Custom hint to bypass plugins on DML queries
            "BYPASS_CUSTOM_PLUGIN_EXECUTION",

            // Custom hint to use cached record count for simple count(*) queries
            "RETRIEVE_TOTAL_RECORD_COUNT",

            // Custom hint to get query plan without any optimization
            "DEBUG_BYPASS_OPTIMIZATION",

            // Custom hint to disable logic to automatically navigate restricted state transitions
            "DISABLE_STATE_TRANSITIONS",

            // Custom hint to keep trying all records in a DML batch even if one fails
            "CONTINUE_ON_ERROR",

            // Custom hint to use legacy specialized update messages - https://learn.microsoft.com/en-us/power-apps/developer/data-platform/org-service/entity-operations-update-delete?tabs=late#legacy-update-messages
            "USE_LEGACY_UPDATE_MESSAGES",

            // Ignore duplicate keys on insert, equivalent to IGNORE_DUP_KEY option on creation of index
            "IGNORE_DUP_KEY",
        };

        private static readonly HashSet<string> _removableSql4CdsQueryHints = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // DML-related hint can be removed from the SELECT statement sent to the TDS Endpoint
            "BYPASS_CUSTOM_PLUGIN_EXECUTION",
        };

        private static readonly string[] _tsqlQueryHintPrefixes = new[]
        {
            "QUERY_OPTIMIZER_COMPATIBILITY_LEVEL_",
        };

        private static readonly string[] _sql4cdsQueryHintPrefixes = new[]
        {
            // Custom hint to set the default page size of FetchXML queries
            "FETCHXML_PAGE_SIZE_",

            // Custom hint to set the batch size for DML queries
            "BATCH_SIZE_",
        };

        private readonly bool _removeSql4CdsHints;

        public OptimizerHintValidatingVisitor(bool removeSql4CdsHints)
        {
            _removeSql4CdsHints = removeSql4CdsHints;
        }

        public bool TdsCompatible { get; private set; } = true;

        public override void ExplicitVisit(UseHintList node)
        {
            base.ExplicitVisit(node);

            var toRemove = new List<StringLiteral>();

            foreach (var hint in node.Hints)
            {
                if (_tsqlQueryHints.Contains(hint.Value))
                    continue;

                if (_sql4cdsQueryHints.Contains(hint.Value))
                {
                    if (_removeSql4CdsHints && _removableSql4CdsQueryHints.Contains(hint.Value))
                        toRemove.Add(hint);
                    else
                        TdsCompatible = false;

                    continue;
                }

                // Some hints allow a numeric suffix
                if (ValidatePrefixHint(hint, _tsqlQueryHintPrefixes))
                    continue;

                if (ValidatePrefixHint(hint, _sql4cdsQueryHintPrefixes))
                {
                    if (_removeSql4CdsHints)
                        toRemove.Add(hint);
                    else
                        TdsCompatible = false;

                    continue;
                }

                throw new NotSupportedQueryFragmentException("Unknown hint", hint);
            }

            foreach (var hint in toRemove)
                node.Hints.Remove(hint);
        }

        public override void ExplicitVisit(SelectStatement node)
        {
            base.ExplicitVisit(node);

            if (_removeSql4CdsHints && node.OptimizerHints != null)
            {
                foreach (var emptyHintList in node.OptimizerHints.OfType<UseHintList>().Where(list => list.Hints.Count == 0).ToList())
                    node.OptimizerHints.Remove(emptyHintList);
            }
        }

        private bool ValidatePrefixHint(StringLiteral hint, string[] queryHintPrefixes)
        {
            var prefix = queryHintPrefixes.FirstOrDefault(p => hint.Value.StartsWith(p, StringComparison.OrdinalIgnoreCase));

            if (prefix == null)
                return false;

            if (!Int32.TryParse(hint.Value.Substring(prefix.Length), out _))
                throw new NotSupportedQueryFragmentException($"Whole number must be specified for {prefix}n hint", hint);

            return true;
        }
    }
}
