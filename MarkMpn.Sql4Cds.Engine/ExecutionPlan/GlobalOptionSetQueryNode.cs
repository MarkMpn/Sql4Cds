using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
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
            public DataTypeReference SqlType { get; set; }
            public Type NetType { get; set; }
            public IComparable[] DataMemberOrder { get; set; }
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
                    DataTypeReference type = null;

                    foreach (var prop in g)
                    {
                        if (type == null)
                        {
                            type = MetadataQueryNode.GetPropertyType(prop.Property.PropertyType);
                        }
                        else if (!SqlTypeConverter.CanMakeConsistentTypes(type, MetadataQueryNode.GetPropertyType(prop.Property.PropertyType), null, out type))
                        {
                            // Can't make a consistent type for this property, so we can't use it
                            type = null;
                            break;
                        }
                    }

                    if (type == null)
                        return null;

                    var netType = type.ToNetType(out _);

                    return new OptionSetProperty
                    {
                        Name = g.Key.ToLowerInvariant(),
                        SqlType = type,
                        NetType = netType,
                        Accessors = g.ToDictionary(p => p.Type, p => MetadataQueryNode.GetPropertyAccessor(p.Property, netType)),
                        DataMemberOrder = MetadataQueryNode.GetDataMemberOrder(g.First().Property)
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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
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

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return new RowCountEstimate(100);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);

            var props = (IEnumerable<OptionSetProperty>)(_optionsetCols ?? _optionsetProps).Values;

            if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                props = props.OrderBy(p => p.Name);
            else
                props = props.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

            foreach (var prop in props)
            {
                schema[$"{Alias}.{prop.Name}"] = new ColumnDefinition(prop.SqlType, true, false);

                if (!aliases.TryGetValue(prop.Name, out var a))
                {
                    a = new List<string>();
                    aliases[prop.Name] = a;
                }

                ((List<string>)a).Add($"{Alias}.{prop.Name}");
            }

            return new NodeSchema(
                primaryKey: $"{Alias}.{nameof(OptionSetMetadataBase.MetadataId)}",
                schema: schema,
                aliases: aliases,
                sortOrder: Array.Empty<string>()
                );
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var resp = (RetrieveAllOptionSetsResponse)dataSource.Connection.Execute(new RetrieveAllOptionSetsRequest());

            foreach (var optionset in resp.OptionSetMetadata)
            {
                var converted = new Entity("globaloptionset", optionset.MetadataId ?? Guid.Empty);

                foreach (var col in _optionsetCols)
                {
                    if (!col.Value.Accessors.TryGetValue(optionset.GetType(), out var optionsetProp))
                    {
                        converted[col.Key] = SqlTypeConverter.GetNullValue(col.Value.NetType);
                        continue;
                    }

                    converted[col.Key] = optionsetProp(optionset);
                }

                yield return converted;
            }
        }

        public override object Clone()
        {
            return new GlobalOptionSetQueryNode
            {
                Alias = Alias,
                DataSource = DataSource,
                _optionsetCols = _optionsetCols
            };
        }
    }
}
