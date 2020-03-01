using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Sixth, take only the TOP x records
    /// </summary>
    class Top : IQueryExtension
    {
        private readonly int _top;

        public Top(int top)
        {
            _top = top;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            return source.Take(_top);
        }
    }
}
