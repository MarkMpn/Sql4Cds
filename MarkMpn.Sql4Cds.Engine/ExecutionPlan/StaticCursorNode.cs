using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class StaticCursorNode : CursorBaseNode
    {
        public override object Clone()
        {
            throw new NotImplementedException();
        }

        public static StaticCursorNode FromQuery(IRootExecutionPlanNodeInternal query)
        {
            // TODO: Handle the query being a SqlNode using TDS Endpoint instead of a SelectNode

            // Cache the results of the query in a temporary table, indexed by the row number
            // TODO: Requires new functionality for temporary table storage, indexes, operators to read & write
            // the temporary tables, Segment and Sequence Project operators to generate row numbers
            throw new NotImplementedException();
        }
    }
}
