using MarkMpn.Sql4Cds.Engine.FetchXml;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Sorts the elements within an &lt;entity&gt; or &lt;link-entity&gt; element to match the order they're commonly
    /// shown in online samples
    /// </summary>
    class FetchXmlElementComparer : IComparer<object>
    {
        private static readonly Type[] ExpectedOrder = new[]
        {
            typeof(allattributes),
            typeof(FetchAttributeType),
            typeof(FetchLinkEntityType),
            typeof(filter),
            typeof(FetchOrderType)
        };

        public int Compare(object x, object y)
        {
            var typeX = x.GetType();
            var typeY = y.GetType();

            var indexX = Array.IndexOf(ExpectedOrder, typeX);
            var indexY = Array.IndexOf(ExpectedOrder, typeY);

            return indexX - indexY;
        }
    }

    /// <summary>
    /// Provides a simple stable sort implementation
    /// </summary>
    static class ArrayExtensions
    {
        /// <summary>
        /// Sort the array elements, keeping the original ordering where two elements are considered equal
        /// </summary>
        /// <typeparam name="T">The type of element being sorted</typeparam>
        /// <param name="array">The array to sort</param>
        /// <param name="comparer">The comparison to apply during sorting</param>
        public static void StableSort<T>(this T[] array, IComparer<T> comparer)
        {
            var sorted = array.Select((obj, i) => new KeyValuePair<int, T>(i, obj))
                .OrderBy(kvp => kvp.Value, comparer)
                .ThenBy(kvp => kvp.Key)
                .ToArray();

            for (var i = 0; i < array.Length; i++)
                array[i] = sorted[i].Value;
        }
    }
}
