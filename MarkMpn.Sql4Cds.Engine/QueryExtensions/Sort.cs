using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Fifth, apply any sorts from the ORDER BY clause
    /// </summary>
    class Sort : IQueryExtension
    {
        private readonly SortExpression[] _sorts;

        public Sort(SortExpression[] sorts)
        {
            _sorts = sorts;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            List<Entity> block = null;

            // Retrieve all the entities with equal values for any sorts that have been applied in FetchXML
            foreach (var entity in source)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                if (block == null)
                    block = new List<Entity>();

                if (block.Count == 0)
                {
                    block.Add(entity);
                }
                else
                {
                    var sameBlock = true;

                    foreach (var sort in _sorts)
                    {
                        if (!sort.FetchXmlSorted)
                            break;

                        var blockValue = sort.Selector(block[0]);
                        var newValue = sort.Selector(entity);

                        if (!Equals(blockValue, newValue))
                        {
                            sameBlock = false;
                            break;
                        }
                    }

                    if (sameBlock)
                    {
                        block.Add(entity);
                    }
                    else
                    {
                        // We've found all the entities with the same FetchXML sort order - finalize the sorting of them with the remaining
                        // sorts and return them.
                        var sorted = OrderBy(block, _sorts);

                        foreach (var e in sorted)
                            yield return e;

                        block.Clear();
                        block.Add(entity);
                    }
                }
            }

            // Sort the final block
            var finalSorted = OrderBy(block, _sorts);

            foreach (var e in finalSorted)
                yield return e;
        }

        /// <summary>
        /// Checks if two values are equal
        /// </summary>
        /// <param name="x">The first value to check</param>
        /// <param name="y">The second value to check</param>
        /// <returns><c>true</c> if the values are equal, or <c>false</c> otherwise</returns>
        private static bool Equal(object x, object y)
        {
            if (x == y)
                return true;

            if (x == null ^ y == null)
                return false;

            return x.Equals(y);
        }

        /// <summary>
        /// Sort a sequence of entities
        /// </summary>
        /// <param name="list">The sequence of entities to sort</param>
        /// <param name="sorts">The sorts to apply</param>
        /// <returns>The sorted sequence of entities</returns>
        private IOrderedEnumerable<Entity> OrderBy(IEnumerable<Entity> list, SortExpression[] sorts)
        {
            IOrderedEnumerable<Entity> sorted = null;

            foreach (var sort in sorts)
            {
                if (sorted == null)
                {
                    if (sort.Descending)
                        sorted = list.OrderByDescending(e => sort.Selector(e));
                    else
                        sorted = list.OrderBy(e => sort.Selector(e));
                }
                else
                {
                    if (sort.Descending)
                        sorted = sorted.ThenByDescending(e => sort.Selector(e));
                    else
                        sorted = sorted.ThenBy(e => sort.Selector(e));
                }
            }

            return sorted;
        }
    }

    public class SortExpression
    {
        public SortExpression(bool fetchXmlSorted, Func<Entity, object> selector, bool descending)
        {
            FetchXmlSorted = fetchXmlSorted;
            Selector = selector;
            Descending = descending;
        }

        public bool FetchXmlSorted { get; }

        public Func<Entity, object> Selector { get; }

        public bool Descending { get; }
    }
}
