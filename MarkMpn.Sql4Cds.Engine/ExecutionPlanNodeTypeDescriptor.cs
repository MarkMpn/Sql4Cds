﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Drawing.Design;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    public class ExecutionPlanNodeTypeDescriptor : CustomTypeDescriptor
    {
        private readonly object _obj;
        private readonly bool _estimated;
        private readonly Func<string, object> _connections;

        public ExecutionPlanNodeTypeDescriptor(object obj, bool estimated, Func<string, object> connections)
        {
            _obj = obj;
            _estimated = estimated;
            _connections = connections;
        }

        public override AttributeCollection GetAttributes()
        {
            return new AttributeCollection(new ReadOnlyAttribute(true));
        }

        public override PropertyDescriptorCollection GetProperties(Attribute[] attributes)
        {
            return new PropertyDescriptorCollection(_obj
                .GetType()
                .GetProperties()
                .Where(p => p.GetIndexParameters().Length == 0)
                .Where(p => p.PropertyType != typeof(ExtensionDataObject))
                .Where(p => p.PropertyType != typeof(IExecutionPlanNode))
                .Where(p => p.DeclaringType != typeof(TSqlFragment))
                .Where(p => p.GetCustomAttribute<BrowsableAttribute>()?.Browsable != false)
                .Where(p => (p.GetCustomAttribute<BrowsableInEstimatedPlanAttribute>()?.Estimated ?? _estimated) == _estimated)
                .Select(p => new WrappedPropertyDescriptor(_obj, p, _estimated, _connections))
                .ToArray());
        }

        public override object GetPropertyOwner(PropertyDescriptor pd)
        {
            return _obj;
        }

        public override string GetClassName()
        {
            if (_obj is IExecutionPlanNode)
                return _obj.ToString();

            return base.GetClassName();
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    class BrowsableInEstimatedPlanAttribute : Attribute
    {
        public BrowsableInEstimatedPlanAttribute(bool estimated)
        {
            Estimated = estimated;
        }

        public bool Estimated { get; }
    }

    class WrappedPropertyDescriptor : PropertyDescriptor
    {
        private readonly object _target;
        private readonly PropertyInfo _prop;
        private readonly object _value;
        private readonly bool _estimated;
        private readonly Func<string, object> _connections;
        private readonly string _originalValue;

        public WrappedPropertyDescriptor(object target, PropertyInfo prop, bool estimated, Func<string, object> connections) : base(prop.Name, GetAttributes(target, prop, connections))
        {
            _target = target;
            _prop = prop;
            _value = prop.GetValue(target);
            _estimated = estimated;
            _connections = connections;

            if (prop.Name == "DataSource" && Category == "Data Source" && PropertyType == typeof(string) && connections != null && connections((string)_value) != null)
            {
                _prop = null;
                _originalValue = (string)_value;
                _value = connections(_originalValue);
            }
        }

        public WrappedPropertyDescriptor(object target, string name, object value) : base(name, GetAttributes(value.GetType()))
        {
            _target = target;
            _value = value;
        }

        private static Attribute[] GetAttributes(object target, PropertyInfo prop, Func<string, object> connections)
        {
            var baseAttributes = Attribute.GetCustomAttributes(prop);
            var value = prop.GetValue(target);

            if (prop.Name == "DataSource" && baseAttributes.OfType<CategoryAttribute>().FirstOrDefault()?.Category == "Data Source" && prop.PropertyType == typeof(string) && connections != null && connections((string)value) != null)
                value = connections((string)value);

            if (value != null)
                return GetAttributes(value.GetType()).Where(a1 => !baseAttributes.Any(a2 => a1.GetType() == a2.GetType())).Concat(baseAttributes).ToArray();

            return GetAttributes(prop.PropertyType).Where(a1 => !baseAttributes.Any(a2 => a1.GetType() == a2.GetType())).Concat(baseAttributes).ToArray();
        }

        public static string GetName(ITypeDescriptorContext context)
        {
            if (context.PropertyDescriptor is WrappedPropertyDescriptor pd && pd._prop == null)
            {
                if (pd._value is string[] strings)
                    return String.Join(", ", strings);

                return $"({context.PropertyDescriptor.PropertyType.Name})";
            }

            return $"({context.PropertyDescriptor.Name})";
        }

        public static Attribute[] GetAttributes(Type type)
        {
            var attrs = new List<Attribute>();

            if ((type.IsClass || type.IsInterface) && type != typeof(string))
            {
                Type typeConverter;

                if (typeof(System.Collections.IList).IsAssignableFrom(type))
                    typeConverter = typeof(DataCollectionConverter);
                else if (typeof(System.Collections.IDictionary).IsAssignableFrom(type))
                    typeConverter = typeof(DictionaryConverter);
                else if (typeof(MultiPartIdentifier).IsAssignableFrom(type))
                    typeConverter = typeof(MultiPartIdentifierConverter);
                else
                    typeConverter = typeof(SimpleNameExpandableObjectConverter);

                attrs.Add(new TypeConverterAttribute(typeConverter));
            }
            else if (type.IsEnum)
            {
                attrs.Add(new TypeConverterAttribute(typeof(EnumConverter)));
            }

            attrs.Add(new ReadOnlyAttribute(true));
            return attrs.ToArray();
        }
        
        public override TypeConverter Converter
        {
            get
            {
                var propTypeConverterAttr = _prop?.GetCustomAttribute<TypeConverterAttribute>();
                if (propTypeConverterAttr != null)
                    return (TypeConverter)Activator.CreateInstance(Type.GetType(propTypeConverterAttr.ConverterTypeName));

                var type = _value?.GetType() ?? _prop.PropertyType;

                if ((type.IsClass || type.IsInterface) && type != typeof(string))
                {
                    if (typeof(System.Collections.IList).IsAssignableFrom(type))
                        return new DataCollectionConverter();

                    if (typeof(System.Collections.IDictionary).IsAssignableFrom(type))
                        return new DictionaryConverter();

                    if (typeof(MultiPartIdentifier).IsAssignableFrom(type))
                        return new MultiPartIdentifierConverter();

                    string name;

                    if (_originalValue != null)
                        name = _originalValue;
                    else if (_value is TSqlFragment sql)
                        name = sql.ToSql();
                    else if (_prop == null || (_value != null && _prop != null && type != _prop.PropertyType))
                    {
                        if (type == typeof(string[]))
                            name = String.Join(", ", (string[])_value);
                        else
                            name = $"({type.Name})";
                    }
                    else
                        name = $"({Name})";

                    return new SimpleNameExpandableObjectConverter(name);
                }
                else if (type.IsEnum)
                {
                    return new EnumConverter(type);
                }

                return base.Converter;
            }
        }

        public override Type ComponentType => _target.GetType();

        public override bool IsReadOnly => true;

        public override Type PropertyType => _prop?.PropertyType ?? _value.GetType();

        public override bool CanResetValue(object component)
        {
            return false;
        }

        public override object GetValue(object component)
        {
            if (_value == null)
                return null;

            var type = _value.GetType();

            if (type.IsClass && type != typeof(string))
                return new ExecutionPlanNodeTypeDescriptor(_value, _estimated, _connections);

            return _value;
        }

        public override void ResetValue(object component)
        {
            throw new NotImplementedException();
        }

        public override void SetValue(object component, object value)
        {
            throw new NotImplementedException();
        }

        public override bool ShouldSerializeValue(object component)
        {
            return false;
        }
    }

    class SimpleNameExpandableObjectConverter : ExpandableObjectConverter
    {
        private readonly string _name;

        public SimpleNameExpandableObjectConverter(string name)
        {
            _name = name;
        }

        public SimpleNameExpandableObjectConverter()
        {
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (destinationType == typeof(string))
            {
                if (value == null)
                    return String.Empty;

                if (_name != null)
                    return _name;

                return WrappedPropertyDescriptor.GetName(context);
            }

            return base.ConvertTo(context, culture, value, destinationType);
        }
    }

    class DataCollectionConverter : TypeConverter
    {
        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value == null)
                return String.Empty;

            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var list = (System.Collections.IList)value;

            if (list.Count == 0)
                return "(None)";

            return WrappedPropertyDescriptor.GetName(context);
        }

        public override bool GetPropertiesSupported(ITypeDescriptorContext context)
        {
            var value = context.PropertyDescriptor.GetValue(context.Instance);

            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var list = (System.Collections.IList)value;
            return list != null && list.Count > 0;
        }

        public override PropertyDescriptorCollection GetProperties(ITypeDescriptorContext context, object value, Attribute[] attributes)
        {
            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var list = (System.Collections.IList)value;

            return new PropertyDescriptorCollection(list.Cast<object>().Select((item, i) => new DataCollectionItemPropertyDescriptor(list, item, i)).ToArray());
        }
    }

    class DataCollectionItemPropertyDescriptor : WrappedPropertyDescriptor
    {
        public DataCollectionItemPropertyDescriptor(System.Collections.IList list, object item, int index) : base(list, GetPropertyName(list, item, index), GetPropertyValue(item))
        {
        }

        private static string GetPropertyName(System.Collections.IList list, object item, int index)
        {
            var dictionaryKeyProperty = item.GetType()
                .GetProperties()
                .SingleOrDefault(p => p.GetCustomAttribute<DictionaryKeyAttribute>() != null);

            if (dictionaryKeyProperty != null)
                return dictionaryKeyProperty.GetValue(item)?.ToString() ?? " ";

            return index.ToString().PadLeft((int)Math.Ceiling(Math.Log10(list.Count)), '0');
        }

        private static object GetPropertyValue(object item)
        {
            var dictionaryValueProperty = item.GetType()
                .GetProperties()
                .SingleOrDefault(p => p.GetCustomAttribute<DictionaryValueAttribute>() != null);

            if (dictionaryValueProperty != null)
                item = dictionaryValueProperty.GetValue(item);
            
            if (item.GetType().IsArray && ((Array)item).Length == 1)
                item = ((Array)item).GetValue(0);

            return item;
        }
    }

    class DictionaryConverter : TypeConverter
    {
        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value == null)
                return String.Empty;

            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var list = (System.Collections.IDictionary)value;

            if (list == null || list.Count == 0)
                return "(None)";

            return WrappedPropertyDescriptor.GetName(context);
        }

        public override bool GetPropertiesSupported(ITypeDescriptorContext context)
        {
            var value = context.PropertyDescriptor.GetValue(context.Instance);

            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var dict = (System.Collections.IDictionary)value;
            return dict != null && dict.Count > 0;
        }

        public override PropertyDescriptorCollection GetProperties(ITypeDescriptorContext context, object value, Attribute[] attributes)
        {
            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var dict = (System.Collections.IDictionary)value;

            return new PropertyDescriptorCollection(dict.Keys.OfType<object>().OrderBy(key => key).Select(key => new WrappedPropertyDescriptor(dict, key.ToString(), dict[key])).ToArray());
        }
    }

    class MultiPartIdentifierConverter : TypeConverter
    {
        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return destinationType == typeof(string);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var id = (MultiPartIdentifier)value;

            if (id == null)
                return String.Empty;

            return String.Join(".", id.Identifiers.Select(i => i.Value));
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    class DictionaryKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    class DictionaryValueAttribute : Attribute
    {
    }

#if !NETCOREAPP
    class MiniChartEditor : UITypeEditor
    {
        public override bool GetPaintValueSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        public override void PaintValue(PaintValueEventArgs e)
        {
            var values = (float[])((ExecutionPlanNodeTypeDescriptor)e.Value).GetPropertyOwner(null);

            if (values.Length == 0)
            return;
            
            var brush = Brushes.DarkBlue;
            var barWidth = (float)e.Bounds.Width / values.Length;
            var maxValue = values.Max();

            for (var i = 0; i < values.Length && maxValue > 0; i++)
            {
                var height = e.Bounds.Height * values[i] / maxValue;
                e.Graphics.FillRectangle(brush, e.Bounds.X + i * barWidth, e.Bounds.Bottom - height, barWidth, height);
            }
        }
    }
#endif

    class MiniChartConverter : DataCollectionConverter
    {
        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var values = (float[])value;
            var maxValue = values.Max();
            var minValue = values.Min();

            if (minValue == maxValue)
                return minValue.ToString();
            else
                return $"{minValue} - {maxValue}";
        }
    }
}
