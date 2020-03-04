using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Add any calculated fields from the SELECT clause
    /// </summary>
    class Projection : IQueryExtension
    {
        private readonly IDictionary<string, Func<Entity, object>> _calculatedFields;

        /// <summary>
        /// Creates a new <see cref="Projection"/>
        /// </summary>
        /// <param name="calculatedFields">A dictionary mapping the requested field name to a function that calculates the value for the field from a <see cref="Entity"/></param>
        public Projection(IDictionary<string, Func<Entity, object>> calculatedFields)
        {
            _calculatedFields = calculatedFields;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            foreach (var entity in source)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                foreach (var field in _calculatedFields)
                    entity[field.Key] = field.Value(entity);

                yield return entity;
            }
        }
    }
}
