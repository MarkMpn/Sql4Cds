using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Apply any custom filters from the HAVING clause
    /// </summary>
    class Having : IQueryExtension
    {
        private readonly Func<Entity, bool> _predicate;

        /// <summary>
        /// Creates a new <see cref="Having"/>
        /// </summary>
        /// <param name="predicate">The predicate to apply to the grouped &amp; aggregated data</param>
        public Having(Func<Entity, bool> predicate)
        {
            _predicate = predicate;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Where(_predicate);
        }
    }
}
