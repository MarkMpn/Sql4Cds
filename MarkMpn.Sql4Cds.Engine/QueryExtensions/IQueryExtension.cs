using Microsoft.Xrm.Sdk;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    public interface IQueryExtension
    {
        IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options);
    }
}
