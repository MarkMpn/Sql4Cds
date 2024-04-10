﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    abstract class ReflectionObjectBase
    {
        protected ReflectionObjectBase(object obj)
        {
            Target = obj;
        }

        public object Target { get; }

        protected static Type GetType(string typeName)
        {
            var type = Type.GetType(typeName);

            if (type == null)
                VsShellUtilities.LogError("SQL 4 CDS", $"Missing type {typeName}");

            return type;
        }

        protected object GetField(object target, string fieldName)
        {
            var field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            if (field == null)
                VsShellUtilities.LogError("SQL 4 CDS", $"Missing field {fieldName} on type {target.GetType()}");

            return field.GetValue(target);
        }

        protected void SetField(object target, string fieldName, object value)
        {
            var field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            if (field == null)
                VsShellUtilities.LogError("SQL 4 CDS", $"Missing field {fieldName} on type {target.GetType()}");

            field.SetValue(target, value);
        }

        protected object GetProperty(object target, string propName)
        {
            var prop = target.GetType().GetProperty(propName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            if (prop == null)
                VsShellUtilities.LogError("SQL 4 CDS", $"Missing property {propName} on type {target.GetType()}");

            return prop.GetValue(target);
        }

        protected void SetProperty(object target, string propName, object value)
        {
            var prop = target.GetType().GetProperty(propName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            if (prop == null)
                VsShellUtilities.LogError("SQL 4 CDS", $"Missing property {propName} on type {target.GetType()}");

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
                    .ToArray();

                if (method.Length == 1)
                {
                    try
                    {
                        return method[0].Invoke(target, args);
                    }
                    catch (TargetInvocationException ex)
                    {
                        throw ex.InnerException;
                    }
                }

                if (method.Length > 1)
                {
                    VsShellUtilities.LogError("SQL 4 CDS", $"Ambiguous method {methodName} on type {target.GetType()}");
                    throw new ArgumentOutOfRangeException(nameof(methodName));
                }

                type = type.BaseType;
            }

            VsShellUtilities.LogError("SQL 4 CDS", $"Missing method {methodName} on type {target.GetType()}");
            throw new ArgumentOutOfRangeException(nameof(methodName));
        }
    }
}
