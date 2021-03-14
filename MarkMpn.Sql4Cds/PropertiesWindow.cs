using System;
using System.Collections.Generic;
using System.Drawing;
using System.Data;
using System.Linq;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds
{
    public partial class PropertiesWindow : WeifenLuo.WinFormsUI.Docking.DockContent
    {

        public PropertiesWindow()
        {
            InitializeComponent();
        }

        public void SelectObject(object obj)
        {
            if (obj != null)
                obj = new TestTypeDescriptor(obj);

            propertyGrid.SelectedObject = obj;
        }
    }

    class TestTypeDescriptor : CustomTypeDescriptor
    {
        private readonly object _obj;

        public TestTypeDescriptor(object obj)
        {
            _obj = obj;
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
                .Where(p => p.DeclaringType != typeof(BaseNode))
                .Where(p => p.DeclaringType != typeof(TSqlFragment))
                .Where(p => p.GetCustomAttribute<BrowsableAttribute>()?.Browsable != false)
                .Select(p => new TestPropertyDescriptor(_obj, p))
                .ToArray());
        }

        public override object GetPropertyOwner(PropertyDescriptor pd)
        {
            return _obj;
        }
    }

    class TestPropertyDescriptor : PropertyDescriptor
    {
        private readonly object _target;
        private readonly PropertyInfo _prop;

        public TestPropertyDescriptor(object target, PropertyInfo prop) : base(prop.Name, GetAttributes(target, prop))
        {
            _target = target;
            _prop = prop;
        }

        private static Attribute[] GetAttributes(object target, PropertyInfo prop)
        {
            var value = prop.GetValue(target);

            if (value != null)
                return GetAttributes(value.GetType());

            return GetAttributes(prop.PropertyType);
        }

        public static Attribute[] GetAttributes(Type type)
        {
            var attrs = new List<Attribute>();

            if ((type.IsClass || type.IsInterface) && type != typeof(string))
            {
                Type typeConverter;

                if (typeof(System.Collections.IList).IsAssignableFrom(type))
                    typeConverter = typeof(DataCollectionConverter);
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
                var val = _prop.GetValue(_target);
                var type = val?.GetType() ?? _prop.PropertyType;

                if ((type.IsClass || type.IsInterface) && type != typeof(string))
                {
                    if (typeof(System.Collections.IList).IsAssignableFrom(type))
                        return new DataCollectionConverter();

                    if (typeof(MultiPartIdentifier).IsAssignableFrom(type))
                        return new MultiPartIdentifierConverter();

                    return new SimpleNameExpandableObjectConverter(val != null && type != _prop.PropertyType ? $"({type.Name})" : $"({Name})");
                }

                return base.Converter;
            }
        }

        public override Type ComponentType => _target.GetType();

        public override bool IsReadOnly => true;

        public override Type PropertyType => _prop.PropertyType;

        public override bool CanResetValue(object component)
        {
            return false;
        }

        public override object GetValue(object component)
        {
            var val = _prop.GetValue(_target);

            if (val == null)
                return null;

            var type = val.GetType();

            if (type.IsClass && type != typeof(string))
                return new TestTypeDescriptor(val);

            return val;
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

                return $"({context.PropertyDescriptor.Name})";
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

            return $"({context.PropertyDescriptor.Name})";
        }

        public override bool GetPropertiesSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        public override PropertyDescriptorCollection GetProperties(ITypeDescriptorContext context, object value, Attribute[] attributes)
        {
            if (value is ICustomTypeDescriptor desc)
                value = desc.GetPropertyOwner(null);

            var list = (System.Collections.IList)value;

            return new PropertyDescriptorCollection(list.Cast<object>().Select((item, i) => new DataCollectionItemPropertyDescriptor(item, i, list.Count)).ToArray());
        }
    }

    class DataCollectionItemPropertyDescriptor : PropertyDescriptor
    {
        private readonly object _item;

        public DataCollectionItemPropertyDescriptor(object item, int index, int count) : base(index.ToString().PadLeft((int)Math.Ceiling(Math.Log10(count)), '0'), TestPropertyDescriptor.GetAttributes(item.GetType()))
        {
            _item = item;
        }

        public override TypeConverter Converter
        {
            get
            {
                var type = _item.GetType();

                if ((type.IsClass || type.IsInterface) && type != typeof(string))
                {
                    if (typeof(System.Collections.IList).IsAssignableFrom(type))
                        return new DataCollectionConverter();

                    if (typeof(MultiPartIdentifier).IsAssignableFrom(type))
                        return new MultiPartIdentifierConverter();

                    return new SimpleNameExpandableObjectConverter($"({type.Name})");
                }

                return base.Converter;
            }
        }

        public override Type ComponentType => typeof(System.Collections.IList);

        public override bool IsReadOnly => true;

        public override Type PropertyType => _item.GetType();

        public override bool CanResetValue(object component)
        {
            return false;
        }

        public override object GetValue(object component)
        {
            var type = _item.GetType();

            if (type.IsClass && type != typeof(string))
                return new TestTypeDescriptor(_item);

            return _item;
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
}
