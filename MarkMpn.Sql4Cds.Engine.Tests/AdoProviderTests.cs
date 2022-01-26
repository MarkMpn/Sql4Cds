using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class AdoProviderTests : FakeXrmEasyTestsBase, IQueryExecutionOptions
    {
        bool IQueryExecutionOptions.Cancelled => false;

        bool IQueryExecutionOptions.BlockUpdateWithoutWhere => false;

        bool IQueryExecutionOptions.BlockDeleteWithoutWhere => false;

        bool IQueryExecutionOptions.UseBulkDelete => false;

        int IQueryExecutionOptions.BatchSize => 1;

        bool IQueryExecutionOptions.UseTDSEndpoint => false;

        bool IQueryExecutionOptions.UseRetrieveTotalRecordCount => true;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.UseLocalTimeZone => false;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        void IQueryExecutionOptions.RetrievingNextPage()
        {
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmInsert(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmUpdate(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmDelete(int count, EntityMetadata meta)
        {
            return true;
        }

        string IQueryExecutionOptions.PrimaryDataSource => "uat";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        [TestMethod]
        public void SelectArithmetic()
        {
            var query = "SELECT employees + 1 AS a, employees * 2 AS b, turnover / 3 AS c, turnover - 4 AS d, turnover / employees AS e FROM account";

            var id = Guid.NewGuid();
            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id] = new Entity("account", id)
                {
                    ["accountid"] = id,
                    ["employees"] = 2,
                    ["turnover"] = new Money(9)
                }
            };

            using (var con = new Sql4CdsConnection(_localDataSource.Values.ToList(), this))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = query;

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt32(0));
                    Assert.AreEqual(4, reader.GetInt32(1));
                    Assert.AreEqual(3M, reader.GetDecimal(2));
                    Assert.AreEqual(5M, reader.GetDecimal(3));
                    Assert.AreEqual(4.5M, reader.GetDecimal(4));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void SelectParameters()
        {
            using (var con = new Sql4CdsConnection(_localDataSource.Values.ToList(), this))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT @param1, @param2";

                cmd.Parameters.Add(new Sql4CdsParameter("@param1", 1));
                cmd.Parameters.Add(new Sql4CdsParameter("@param2", "text"));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual("text", reader.GetString(1));
                    Assert.IsFalse(reader.Read());
                }
            }
        }
    }
}
