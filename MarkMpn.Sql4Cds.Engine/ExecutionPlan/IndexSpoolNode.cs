﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Stores data in a hashtable for fast lookups
    /// </summary>
    class IndexSpoolNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        private IDictionary<object, List<Entity>> _hashTable;

        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        /// <summary>
        /// The column in the data source to create an index on
        /// </summary>
        [Category("Index Spool")]
        [Description("The column in the data source to create an index on")]
        [DisplayName("Key Column")]
        public string KeyColumn { get; set; }

        /// <summary>
        /// The name of the parameter to use for seeking in the index
        /// </summary>
        [Category("Index Spool")]
        [Description("The name of the parameter to use for seeking in the index")]
        [DisplayName("Seek Value")]
        public string SeekValue { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            requiredColumns.Add(KeyColumn);

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize) / 100;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            return this;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            // Build an internal hash table of the source indexed by the key column
            if (_hashTable == null)
            {
                _hashTable = Source.Execute(org, metadata, options, parameterTypes, parameterValues)
                    .GroupBy(e => e[KeyColumn], CaseInsensitiveObjectComparer.Instance)
                    .ToDictionary(g => g.Key, g => g.ToList(), CaseInsensitiveObjectComparer.Instance);
            }

            var keyValue = parameterValues[SeekValue];

            if (!_hashTable.TryGetValue(keyValue, out var matches))
                return Array.Empty<Entity>();

            return matches;
        }

        public override string ToString()
        {
            return "Index Spool\r\n(Eager Spool)";
        }
    }
}
