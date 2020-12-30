using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.SSMS
{
    abstract class ReflectionObjectBase
    {
        protected ReflectionObjectBase(object obj)
        {
            Target = obj;
        }

        protected object Target { get; }

        protected object GetField(object target, string fieldName)
        {
            var field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            return field.GetValue(target);
        }

        protected void SetField(object target, string fieldName, object value)
        {
            var field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            field.SetValue(target, value);
        }

        protected object GetProperty(object target, string propName)
        {
            var prop = target.GetType().GetProperty(propName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            return prop.GetValue(target);
        }

        protected void SetProperty(object target, string propName, object value)
        {
            var prop = target.GetType().GetProperty(propName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            prop.SetValue(target, value);
        }

        protected object InvokeMethod(object target, string methodName, params object[] args)
        {
            var type = target.GetType();

            while (type != null)
            {
                var methods = type.GetMethods(BindingFlags.InvokeMethod | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                var method = methods
                    .Where(m => m.Name == methodName && m.GetParameters().Length == args.Length)
                    .SingleOrDefault();

                if (method != null)
                    return method.Invoke(target, args);

                type = type.BaseType;
            }

            throw new ArgumentOutOfRangeException(nameof(methodName));
        }
    }
}
