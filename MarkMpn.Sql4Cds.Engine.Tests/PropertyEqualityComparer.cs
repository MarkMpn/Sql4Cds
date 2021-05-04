using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    static class PropertyEqualityAssert
    {
        public static void Equals(object x, object y)
        {
            Equals("root", x, y);
        }

        private static void Equals(string route, object x, object y)
        {
            if (x == null && y == null)
                return;

            if (x == null || y == null)
                Assert.Fail($"{route} is null for one object and {x ?? y} for the other");

            var typeX = x.GetType();
            var typeY = y.GetType();

            if (typeX != typeY)
                Assert.Fail($"{route} is {typeX} for one object and {typeY} for the other");

            if (typeX == typeof(string) || typeX.IsPrimitive || typeX.IsEnum)
            {
                if (!x.Equals(y))
                    Assert.Fail($"{route} is {x} for one object and {y} for the other");

                return;
            }

            foreach (var prop in typeX.GetProperties())
            {
                if (prop.DeclaringType == typeof(FetchLinkEntityType) && prop.Name == nameof(FetchLinkEntityType.SemiJoin))
                    continue;

                var propRoute = route + "." + prop.Name;

                var valueX = prop.GetValue(x);
                var valueY = prop.GetValue(y);

                // Treat empty arrays as null
                if (valueX != null && valueX.GetType().IsArray && ((Array)valueX).Length == 0)
                    valueX = null;
                if (valueY != null && valueY.GetType().IsArray && ((Array)valueY).Length == 0)
                    valueY = null;

                if (valueX == null && valueY == null)
                    continue;

                if (valueX == null || valueY == null)
                    Assert.Fail($"{propRoute} is null for one object and {valueX ?? valueY} for the other");

                if (!prop.PropertyType.IsArray)
                {
                    Equals(propRoute, valueX, valueY);
                }
                else
                {
                    var arrayX = (Array)valueX;
                    var arrayY = (Array)valueY;

                    var lengthX = arrayX.Length;
                    var lengthY = arrayY.Length;

                    if (lengthX != lengthY)
                        Assert.Fail($"{propRoute} has {lengthX} elements in one object and {lengthY} in the other");

                    for (var i = 0; i < lengthX; i++)
                        Equals($"{propRoute}[{i}]", arrayX.GetValue(i), arrayY.GetValue(i));
                }
            }
        }
    }
}
