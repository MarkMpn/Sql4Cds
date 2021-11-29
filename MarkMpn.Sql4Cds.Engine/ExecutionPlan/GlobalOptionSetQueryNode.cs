using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class GlobalOptionSetQueryNode : BaseDataNode
    {
        class OptionSetProperty
        {
            public string Name { get; set; }
            public IDictionary<Type, Func<object, object>> Accessors { get; set; }
            public Type Type { get; set; }
        }

        private static readonly Type[] _optionsetTypes;
        private static readonly Dictionary<string, OptionSetProperty> _optionsetProps;

        private IDictionary<string, OptionSetProperty> _optionsetCols;

        static GlobalOptionSetQueryNode()
        {
            _optionsetTypes = new[] { typeof(OptionSetMetadata), typeof(BooleanOptionSetMetadata) };

            // Combine the properties available from each optionset type
            _optionsetProps = _optionsetTypes
                .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Select(p => new { Type = t, Property = p }))
                .Where(p => p.Property.Name != nameof(AttributeMetadata.ExtensionData))
                .GroupBy(p => p.Property.Name, p => p, StringComparer.OrdinalIgnoreCase)
                .Select(g =>
                {
                    // Work out the consistent type for each property
                    Type type = null;

                    foreach (var prop in g)
                    {
                        if (type == null)
                        {
                            type = MetadataQueryNode.GetPropertyType(prop.Property.PropertyType);
                        }
                        else if (!SqlTypeConverter.CanMakeConsistentTypes(type, MetadataQueryNode.GetPropertyType(prop.Property.PropertyType), out type))
                        {
                            // Can't make a consistent type for this property, so we can't use it
                            type = null;
                            break;
                        }
                    }

                    if (type == null)
                        return null;

                    return new OptionSetProperty
                    {
                        Name = g.Key.ToLowerInvariant(),
                        Type = type,
                        Accessors = g.ToDictionary(p => p.Type, p => MetadataQueryNode.GetPropertyAccessor(p.Property, type))
                    };
                })
                .Where(p => p != null)
                .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The alias to use for the dataset
        /// </summary>
        [Category("Global Optionset Query")]
        [Description("The alias to use for the dataset")]
        public string Alias { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _optionsetCols = new Dictionary<string, OptionSetProperty>();

            foreach (var col in requiredColumns)
            {
                var parts = col.Split('.');

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(Alias, StringComparison.OrdinalIgnoreCase))
                {
                    var prop = _optionsetProps[parts[1]];
                    _optionsetCols[col] = prop;
                }
            }
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return 100;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();

            foreach (var prop in _optionsetProps.Values)
            {
                schema.Schema[$"{Alias}.{prop.Name}"] = prop.Type;

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

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var resp = (RetrieveAllOptionSetsResponse)dataSource.Connection.Execute(new RetrieveAllOptionSetsRequest());

            foreach (var optionset in resp.OptionSetMetadata)
            {
                var converted = new Entity("globaloptionset", optionset.MetadataId ?? Guid.Empty);

                foreach (var col in _optionsetCols)
                {
                    if (!col.Value.Accessors.TryGetValue(optionset.GetType(), out var optionsetProp))
                    {
                        converted[col.Key] = SqlTypeConverter.GetNullValue(col.Value.Type);
                        continue;
                    }

                    converted[col.Key] = optionsetProp(optionset);
                }

                yield return converted;
            }
        }
    }
}
