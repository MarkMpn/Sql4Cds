using Microsoft.Xrm.Sdk;
using System;
using System.ComponentModel;

namespace MarkMpn.Sql4Cds.Engine
{
    public class SortExpression
    {
        public SortExpression(bool fetchXmlSorted, Func<Entity, IComparable> selector, bool descending)
        {
            FetchXmlSorted = fetchXmlSorted;
            Selector = selector;
            Descending = descending;
        }

        public bool FetchXmlSorted { get; }

        public Func<Entity,IComparable> Selector { get; }

        public bool Descending { get; }
    }
}
