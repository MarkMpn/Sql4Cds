using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    static class EntityExtensions
    {
        /// <summary>
        /// Gets the value of an attribute that may be aliased
        /// </summary>
        /// <typeparam name="T">The type of the value that should be stored in the attribute</typeparam>
        /// <param name="entity">The entity to get the value from</param>
        /// <param name="logicalName">The name of the attribute to get the value of</param>
        /// <returns>The value stored in the attribute</returns>
        public static T GetAliasedAttributeValue<T>(this Entity entity, string logicalName)
        {
            if (!entity.Attributes.TryGetValue(logicalName, out var value))
                return default(T);

            if (value is AliasedValue alias)
                value = alias.Value;

            return (T)value;
        }
    }
}
