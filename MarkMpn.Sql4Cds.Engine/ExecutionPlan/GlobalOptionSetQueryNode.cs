using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
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
            public IDictionary<Type, Func<object, ExpressionExecutionContext, object>> Accessors { get; set; }
            public DataTypeReference SqlType { get; set; }
            public Type NetType { get; set; }
            public IComparable[] DataMemberOrder { get; set; }
        }

        private static readonly Type[] _optionsetTypes;
        private static readonly OptionSetProperty _optionsetIdProp;
        private static readonly Dictionary<string, OptionSetProperty> _optionsetProps;
        private static readonly Dictionary<string, OptionSetProperty> _valueProps;

        private IDictionary<string, OptionSetProperty> _optionsetCols;
        private IDictionary<string, OptionSetProperty> _valueCols;

        static GlobalOptionSetQueryNode()
        {
            _optionsetTypes = new[] { typeof(OptionSetMetadata), typeof(BooleanOptionSetMetadata) };

            // Combine the properties available from each optionset type
            _optionsetProps = _optionsetTypes
                .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Select(p => new { Type = t, Property = p }))
                .Where(p => p.Property.Name != nameof(AttributeMetadata.ExtensionData))
                .Where(p => p.Property.PropertyType != typeof(OptionMetadataCollection))
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
                        else if (!SqlTypeConverter.CanMakeConsistentTypes(type, MetadataQueryNode.GetPropertyType(prop.Property.PropertyType), null, null, null, out type))
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

            _valueProps = typeof(OptionMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.Name != nameof(AttributeMetadata.ExtensionData) && p.PropertyType != typeof(int[]))
                .Select(p =>
                {
                    var type = MetadataQueryNode.GetPropertyType(p.PropertyType);
                    var netType = type.ToNetType(out _);

                    return new OptionSetProperty
                    {
                        Name = p.Name.ToLowerInvariant(),
                        SqlType = type,
                        NetType = netType,
                        Accessors = new Dictionary<Type, Func<object, ExpressionExecutionContext, object>>
                        {
                            [typeof(OptionMetadata)] = MetadataQueryNode.GetPropertyAccessor(p, netType)
                        },
                        DataMemberOrder = MetadataQueryNode.GetDataMemberOrder(p)
                    };
                })
                .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

            // Create a fake property to allow joining options to the parent optionset
            _optionsetIdProp = new OptionSetProperty
            {
                DataMemberOrder = new IComparable[]
                {
                    0,
                    0,
                    "optionsetid"
                },
                Name = "optionsetid",
                SqlType = DataTypeHelpers.UniqueIdentifier,
                NetType = DataTypeHelpers.UniqueIdentifier.ToNetType(out _),
            };
            _valueProps[_optionsetIdProp.Name] = _optionsetIdProp;
        }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The types of metadata to include in the result
        /// </summary>
        [Category("Global Optionset Query")]
        [Description("The types of global optionset metadata to include in the result")]
        [DisplayName("Metadata Source")]
        public OptionSetSource MetadataSource { get; set; }

        /// <summary>
        /// The alias to use for the optionset dataset
        /// </summary>
        [Category("Global Optionset Query")]
        [Description("The alias to use for the optionset dataset")]
        [DisplayName("OptionSet Alias")]
        public string OptionSetAlias { get; set; }

        /// <summary>
        /// The alias to use for the values dataset
        /// </summary>
        [Category("Global Optionset Query")]
        [Description("The alias to use for the values dataset")]
        [DisplayName("Value Alias")]
        public string ValueAlias { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            _optionsetCols = new Dictionary<string, OptionSetProperty>();
            _valueCols = new Dictionary<string, OptionSetProperty>();

            foreach (var col in requiredColumns)
            {
                var parts = col.SplitMultiPartIdentifier();

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(OptionSetAlias, StringComparison.OrdinalIgnoreCase))
                {
                    var prop = _optionsetProps[parts[1]];
                    _optionsetCols[col] = prop;
                }
                else if (parts[0].Equals(ValueAlias, StringComparison.OrdinalIgnoreCase))
                {
                    var prop = _valueProps[parts[1]];
                    _valueCols[col] = prop;
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
            string primaryKey = null;

            if (MetadataSource.HasFlag(OptionSetSource.OptionSet))
                AddSchemaCols(context, schema, aliases, _optionsetCols ?? _optionsetProps, OptionSetAlias, ref primaryKey);

            if (MetadataSource.HasFlag(OptionSetSource.Value))
            {
                AddSchemaCols(context, schema, aliases, _valueCols ?? _valueProps, ValueAlias, ref primaryKey);
                primaryKey = null;
            }

            return new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                sortOrder: Array.Empty<string>()
                );
        }

        private void AddSchemaCols(NodeCompilationContext context, ColumnList schema, Dictionary<string, IReadOnlyList<string>> aliases, IDictionary<string, OptionSetProperty> cols, string alias, ref string primaryKey)
        {
            var props = (IEnumerable<OptionSetProperty>)cols.Values;

            if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                props = props.OrderBy(p => p.Name);
            else
                props = props.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

            foreach (var prop in props)
            {
                var nullable = true;

                if ((alias == OptionSetAlias && prop.Name == nameof(OptionSetMetadata.MetadataId).ToLowerInvariant()) ||
                    (alias == ValueAlias && prop.Name == "optionsetid"))
                {
                    nullable = false;
                }

                schema[$"{alias}.{prop.Name}"] = new ColumnDefinition(prop.SqlType, nullable, false, isWildcardable: true);

                if (!aliases.TryGetValue(prop.Name, out var a))
                {
                    a = new List<string>();
                    aliases[prop.Name] = a;
                }

                ((List<string>)a).Add($"{alias}.{prop.Name}");
            }

            primaryKey = $"{alias}.{nameof(OptionSetMetadataBase.MetadataId)}";
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var resp = (RetrieveAllOptionSetsResponse)dataSource.Connection.Execute(new RetrieveAllOptionSetsRequest());
            var eec = new ExpressionExecutionContext(context);

            var results = resp.OptionSetMetadata.Select(os => new { OptionSet = os, Value = default(OptionMetadata) });

            if (MetadataSource.HasFlag(OptionSetSource.Value))
            {
                results = results.SelectMany(os =>
                {
                    var values = (os.OptionSet as OptionSetMetadata)?.Options;

                    if (values == null && os.OptionSet is BooleanOptionSetMetadata bos)
                    {
                        values = new OptionMetadataCollection
                        {
                            bos.FalseOption,
                            bos.TrueOption
                        };
                    }

                    return values.Select(v => new { os.OptionSet, Value = v });
                });
            }

            foreach (var result in results)
            {
                var converted = new Entity();

                if (MetadataSource.HasFlag(OptionSetSource.OptionSet))
                {
                    converted.LogicalName = "globaloptionset";
                    converted.Id = result.OptionSet.MetadataId ?? Guid.Empty;

                    foreach (var col in _optionsetCols)
                    {
                        // Not all properties are available on all optionset types, so add null values as needed
                        if (!col.Value.Accessors.TryGetValue(result.OptionSet.GetType(), out var optionsetProp))
                        {
                            converted[col.Key] = SqlTypeConverter.GetNullValue(col.Value.NetType);
                            continue;
                        }

                        converted[col.Key] = optionsetProp(result.OptionSet, eec);
                    }
                }

                if (MetadataSource.HasFlag(OptionSetSource.Value))
                {
                    converted.LogicalName = "globaloptionsetvalue";
                    converted.Id = Guid.Empty;

                    foreach (var col in _valueCols)
                    {
                        // Special case for parent optionset id
                        if (col.Value == _optionsetIdProp)
                        {
                            converted[col.Key] = (SqlGuid)(result.OptionSet.MetadataId ?? Guid.Empty);
                            continue;
                        }

                        converted[col.Key] = col.Value.Accessors[typeof(OptionMetadata)](result.Value, eec);
                    }
                }

                yield return converted;
            }
        }

        public override object Clone()
        {
            return new GlobalOptionSetQueryNode
            {
                MetadataSource = MetadataSource,
                OptionSetAlias = OptionSetAlias,
                ValueAlias = ValueAlias,
                DataSource = DataSource,
                _optionsetCols = _optionsetCols,
                _valueCols = _valueCols,
            };
        }
    }

    [Flags]
    public enum OptionSetSource
    {
        OptionSet = 1,
        Value = 2,
    }
}
