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
    public class GlobalOptionSetQueryNode : BaseNode
    {
        private IDictionary<string, string> _optionsetProps;

        public string Alias { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _optionsetProps = new Dictionary<string, string>();

            foreach (var col in requiredColumns)
            {
                var parts = col.Split('.');

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(Alias, StringComparison.OrdinalIgnoreCase))
                {
                    var prop = typeof(OptionSetMetadataBase).GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    _optionsetProps[col] = prop.Name;
                }
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return 100;
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();

            var excludedProps = new[]
            {
                nameof(OptionSetMetadataBase.ExtensionData)
            };

            var optionsetProps = typeof(OptionSetMetadataBase).GetProperties().Where(p => !excludedProps.Contains(p.Name));

            foreach (var prop in optionsetProps)
            {
                var propType = prop.PropertyType;

                if (propType == typeof(Label) || propType.IsEnum || propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>) && propType.GetGenericArguments()[0].IsEnum)
                    propType = typeof(string);

                schema.Schema[$"{Alias}.{prop.Name}"] = propType;

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
            var optionsetProps = typeof(OptionSetMetadataBase).GetProperties().ToDictionary(p => p.Name);

            foreach (var optionset in resp.OptionSetMetadata)
            {
                var converted = new Entity("globaloptionset", optionset.MetadataId ?? Guid.Empty);

                foreach (var prop in _optionsetProps)
                {
                    var value = optionsetProps[prop.Value].GetValue(optionset);

                    if (value is Label l)
                        value = l.UserLocalizedLabel?.Label;
                    else if (value != null && value.GetType().IsEnum)
                        value = value.ToString();

                    converted[prop.Key] = value;
                }

                yield return converted;
            }
        }
    }
}
