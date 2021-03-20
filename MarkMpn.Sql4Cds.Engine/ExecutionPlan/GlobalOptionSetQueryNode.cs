using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class GlobalOptionSetQueryNode : BaseDataNode
    {
        private static readonly Type[] _optionsetTypes = new[] { typeof(OptionSetMetadata), typeof(BooleanOptionSetMetadata) };
        private static readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _optionsetProps = _optionsetTypes.ToDictionary(t => t, t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != nameof(OptionSetMetadataBase.ExtensionData) && p.Name != nameof(OptionSetMetadata.Options)).ToDictionary(p => p.Name));
        private static readonly Dictionary<string, PropertyInfo> _flattenedOptionsetProps = _optionsetProps.SelectMany(kvp => kvp.Value).Select(kvp => kvp.Value).GroupBy(p => p.Name).ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

        private IDictionary<string, string> _optionsetCols;

        public string Alias { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _optionsetCols = new Dictionary<string, string>();

            foreach (var col in requiredColumns)
            {
                var parts = col.Split('.');

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(Alias, StringComparison.OrdinalIgnoreCase))
                {
                    var prop = _flattenedOptionsetProps[parts[1]];
                    _optionsetCols[col] = prop.Name;
                }
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return 100;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();

            foreach (var prop in _flattenedOptionsetProps.Values)
            {
                schema.Schema[$"{Alias}.{prop.Name}"] = MetadataQueryNode.GetPropertyType(prop.PropertyType);

                if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                {
                    aliases = new List<string>();
                    schema.Aliases[prop.Name] = aliases;
                }

                aliases.Add($"{Alias}.{prop.Name}");
            }

            schema.PrimaryKey = $"{Alias}.{nameof(OptionSetMetadataBase.MetadataId)}";

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var resp = (RetrieveAllOptionSetsResponse)org.Execute(new RetrieveAllOptionSetsRequest());

            foreach (var optionset in resp.OptionSetMetadata)
            {
                var converted = new Entity("globaloptionset", optionset.MetadataId ?? Guid.Empty);
                var availableProps = _optionsetProps[optionset.GetType()];

                foreach (var col in _optionsetCols)
                {
                    if (!availableProps.TryGetValue(col.Value, out var optionsetProp))
                    {
                        converted[col.Key] = null;
                        continue;

                    }

                    converted[col.Key] = MetadataQueryNode.GetPropertyValue(optionsetProp, optionset);
                }

                yield return converted;
            }
        }
    }
}
