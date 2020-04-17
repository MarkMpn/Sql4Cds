using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Apply the filter from the WHERE clause to the results produced by the raw FetchXML
    /// </summary>
    class Where : IQueryExtension
    {
        private readonly Func<Entity, bool> _predicate;

        /// <summary>
        /// Creates a new <see cref="Where"/>
        /// </summary>
        /// <param name="predicate">The predicate to use to filter the results</param>
        public Where(Func<Entity, bool> predicate)
        {
            _predicate = predicate;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Where(_predicate);
        }
    }
}
