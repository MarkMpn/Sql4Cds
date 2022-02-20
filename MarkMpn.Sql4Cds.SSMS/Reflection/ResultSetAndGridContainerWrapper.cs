using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.SSMS
{
    class ResultSetAndGridContainerWrapper : ReflectionObjectBase
    {
        private static readonly Type ResultSetAndGridContainer;

        static ResultSetAndGridContainerWrapper()
        {
            ResultSetAndGridContainer = GetType("Microsoft.SqlServer.Management.QueryExecution.ResultSetAndGridContainer, SQLEditors");
        }

        public ResultSetAndGridContainerWrapper(object obj) : base(obj)
        {
        }

        public static ResultSetAndGridContainerWrapper Create(QEResultSetWrapper resultSet, bool printColumnHeaders, int numberOfCharsToShow)
        {
            return new ResultSetAndGridContainerWrapper(Activator.CreateInstance(ResultSetAndGridContainer, resultSet.Target, printColumnHeaders, numberOfCharsToShow));
        }

        public void StartRetrievingData()
        {
            var maxNumCharsToDisplay = 1024;
            var maxNumXmlCharsToDisplay = 1024;
            InvokeMethod(Target, "StartRetrievingData", maxNumCharsToDisplay, maxNumXmlCharsToDisplay);
        }

        public void UpdateGrid()
        {
            InvokeMethod(Target, "UpdateGrid");
        }
    }
}
