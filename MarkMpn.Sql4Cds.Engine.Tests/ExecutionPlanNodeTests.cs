using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FakeXrmEasy;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExecutionPlanNodeTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void ConstantScanTest()
        {
            var node = new ConstantScanNode
            {
                Values =
                {
                    new Entity
                    {
                        ["firstname"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace)
                    }
                },
                Schema =
                {
                    ["firstname"] = typeof(SqlString)
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["firstname"]).Value);
        }

        [TestMethod]
        public void FilterNodeTest()
        {
            var node = new FilterNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["firstname"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["firstname"] = new SqlString("Joe", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["firstname"] = typeof(SqlString)
                    }
                },
                Filter = new BooleanComparisonExpression
                {
                    FirstExpression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = "firstname" }
                            }
                        }
                    },
                    ComparisonType = BooleanComparisonType.Equals,
                    SecondExpression = new StringLiteral { Value = "Mark" }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["firstname"]).Value);
        }

        [TestMethod]
        public void MergeJoinInnerTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["f.key"] = new SqlInt32(1), ["f.firstname"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["f.key"] = new SqlInt32(2), ["f.firstname"] = new SqlString("Joe", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["f.key"] = typeof(SqlInt32),
                        ["f.firstname"] = typeof(SqlString)
                    }
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["l.key"] = new SqlInt32(1), ["l.lastname"] = new SqlString("Carrington", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["l.key"] = new SqlInt32(1), ["l.lastname"] = new SqlString("Twain", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["l.key"] = new SqlInt32(3), ["l.lastname"] = new SqlString("Webber", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["l.key"] = typeof(SqlInt32),
                        ["l.lastname"] = typeof(SqlString)
                    }
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.Inner
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(2, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
        }

        [TestMethod]
        public void MergeJoinLeftOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["key"] = new SqlInt32(1), ["firstname"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(2), ["firstname"] = new SqlString("Joe", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32),
                        ["firstname"] = typeof(SqlString)
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["key"] = new SqlInt32(1), ["lastname"] = new SqlString("Carrington", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(1), ["lastname"] = new SqlString("Twain", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(3), ["lastname"] = new SqlString("Hamill", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32),
                        ["lastname"] = typeof(SqlString)
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.LeftOuter
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
            Assert.AreEqual("Joe", ((SqlString)results[2]["f.firstname"]).Value);
            Assert.IsTrue(((SqlString)results[2]["l.lastname"]).IsNull);
        }

        [TestMethod]
        public void MergeJoinRightOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["key"] = new SqlInt32(1), ["firstname"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(2), ["firstname"] = new SqlString("Joe", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32),
                        ["firstname"] = typeof(SqlString)
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["key"] = new SqlInt32(1), ["lastname"] = new SqlString("Carrington", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(1), ["lastname"] = new SqlString("Twain", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["key"] = new SqlInt32(3), ["lastname"] = new SqlString("Hamill", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32),
                        ["lastname"] = typeof(SqlString)
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.RightOuter
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
            Assert.IsTrue(((SqlString)results[2]["f.firstname"]).IsNull);
            Assert.AreEqual("Hamill", ((SqlString)results[2]["l.lastname"]).Value);
        }

        [TestMethod]
        public void AssertionTest()
        {
            var node = new AssertNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["name"] = new SqlString("Mark", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) },
                        new Entity { ["name"] = new SqlString("Carrington", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace) }
                    }
                },
                Assertion = e => e.GetAttributeValue<SqlString>("name").Value == "Mark",
                ErrorMessage = "Only Mark is allowed"
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).GetEnumerator();

            Assert.IsTrue(results.MoveNext());
            Assert.AreEqual("Mark", results.Current.GetAttributeValue<SqlString>("name").Value);

            var ex = Assert.ThrowsException<QueryExecutionException>(() => results.MoveNext());
            Assert.AreEqual(node.ErrorMessage, ex.Message);
        }

        [TestMethod]
        public void ComputeScalarTest()
        {
            var node = new ComputeScalarNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = new SqlInt32(1), ["value2"] = new SqlInt32(2) },
                        new Entity { ["value1"] = new SqlInt32(3), ["value2"] = new SqlInt32(4) }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(SqlInt32),
                        ["value2"] = typeof(SqlInt32)
                    }
                },
                Columns =
                {
                    ["mul"] = new BinaryExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value1" } }
                            }
                        },
                        BinaryExpressionType = BinaryExpressionType.Multiply,
                        SecondExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value2" } }
                            }
                        }
                    }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("mul").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 2, 12 }, results);
        }

        [TestMethod]
        public void DistinctTest()
        {
            var node = new DistinctNode
            {
                Columns = { "value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = new SqlInt32(1), ["value2"] = new SqlInt32(1) },
                        new Entity { ["value1"] = new SqlInt32(3), ["value2"] = new SqlInt32(2) },
                        new Entity { ["value1"] = new SqlInt32(1), ["value2"] = new SqlInt32(3) }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(SqlInt32),
                        ["value2"] = typeof(SqlInt32)
                    }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 3 }, results);
        }

        [TestMethod]
        public void DistinctCaseInsensitiveTest()
        {
            var node = new DistinctNode
            {
                Columns = { "value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = new SqlString("hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(1) },
                        new Entity { ["value1"] = new SqlString("world", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(2) },
                        new Entity { ["value1"] = new SqlString("Hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(3) }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(SqlString),
                        ["value2"] = typeof(SqlInt32)
                    }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlString>("value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { "hello", "world" }, results);
        }

        [TestMethod]
        public void SortNodeTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = new SqlString("hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(1), ["expectedorder"] = new SqlInt32(2) },
                        new Entity { ["value1"] = new SqlString("world", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(2), ["expectedorder"] = new SqlInt32(3) },
                        new Entity { ["value1"] = new SqlString("Hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(3), ["expectedorder"] = new SqlInt32(1) }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(SqlString),
                        ["value2"] = typeof(SqlInt32),
                        ["expectedorder"] = typeof(SqlInt32)
                    }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("expectedorder").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, results);
        }

        [TestMethod]
        public void SortNodePresortedTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                PresortedCount = 1,
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Entity { ["value1"] = new SqlString("hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(1), ["expectedorder"] = new SqlInt32(1) },
                        new Entity { ["value1"] = new SqlString("world", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(2), ["expectedorder"] = new SqlInt32(2) },
                        new Entity { ["value1"] = new SqlString("Hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(3), ["expectedorder"] = new SqlInt32(4) },
                        new Entity { ["value1"] = new SqlString("Hello", CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace), ["value2"] = new SqlInt32(4), ["expectedorder"] = new SqlInt32(3) }
                    },
                    Schema =
                    {
                        ["value1"] = typeof(SqlString),
                        ["value2"] = typeof(SqlInt32),
                        ["expectedorder"] = typeof(SqlInt32)
                    }
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("expectedorder").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, results);
        }

        [TestMethod]
        public void TableSpoolTest()
        {
            var source = new ConstantScanNode
            {
                Values =
                {
                    new Entity{["value1"] = new SqlInt32(1)},
                    new Entity{["value1"] = new SqlInt32(2)}
                },
                Schema =
                {
                    ["value1"] = typeof(SqlInt32)
                }
            };

            var spool = new TableSpoolNode { Source = source };

            var results1 = spool.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("value1").Value)
                .ToArray();

            var results2 = spool.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2 }, results1);
            CollectionAssert.AreEqual(new[] { 1, 2 }, results2);
            Assert.AreEqual(1, source.ExecutionCount);
        }
    }
}
