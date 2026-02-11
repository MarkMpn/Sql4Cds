using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class OpenRowsetBulkTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void MustSpecifyCorrelationName()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', FORMAT='CSV')";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(491, ex.Number);
            }
        }

        [TestMethod]
        public void MustSpecifySchema()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', FORMAT='CSV') AS t";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(15808, ex.Number);
            }
        }

        [TestMethod]
        public void UnsupportedFormat()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', FORMAT='JSONL')";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(46010, ex.Number);
            }
        }

        [TestMethod]
        public void UnsupportedFormatFile()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', FORMATFILE='format.xml')";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(40517, ex.Number);
            }
        }

        [TestMethod]
        public void UnsupportedDataSource()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', DATA_SOURCE='root')";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(40517, ex.Number);
            }
        }

        [TestMethod]
        public void UnsupportedErrorFileDataSource()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK 'file.csv', ERRORFILE_DATA_SOURCE='root')";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(40517, ex.Number);
            }
        }

        [TestMethod]
        public void InvalidFilename()
        {

            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $@"
SELECT * FROM OPENROWSET(BULK 'invalid.csv', FORMAT='CSV') 
WITH (
    Name varchar(100),
    Latitude float,
    Longitude float,
    Address varchar(max),
    Icon varchar(100),
) AS t";
                var ex = Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteReader());
                Assert.AreEqual(13822, ex.Number);
            }
        }

        [TestMethod]
        public void ReadCsv()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $@"
SELECT * FROM OPENROWSET(BULK '{path}', FORMAT='CSV') 
WITH (
    Name varchar(100),
    Latitude float,
    Longitude float,
    Address varchar(max),
    Icon varchar(100),
) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("Name", reader.GetName(0));
                    Assert.AreEqual("Latitude", reader.GetName(1));
                    Assert.AreEqual("Longitude", reader.GetName(2));
                    Assert.AreEqual("Address", reader.GetName(3));
                    Assert.AreEqual("Icon", reader.GetName(4));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Empire State Building", reader.GetString(0));
                    Assert.AreEqual(40.748817f, reader.GetFloat(1));
                    Assert.AreEqual(-73.985428f, reader.GetFloat(2));
                    Assert.AreEqual("20 W 34th St, New York, NY 10118", reader.GetString(3));
                    Assert.AreEqual("\\icons\\sol.png", reader.GetString(4));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Statue of Liberty", reader.GetString(0));
                    Assert.AreEqual(40.689247f, reader.GetFloat(1));
                    Assert.AreEqual(-74.044502f, reader.GetFloat(2));
                    Assert.AreEqual("Liberty Island, New York, NY 10004", reader.GetString(3));
                    Assert.AreEqual("\\icons\\sol.png", reader.GetString(4));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ReadCsvFirstRow()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $@"
SELECT * FROM OPENROWSET(BULK '{path}', FORMAT='CSV', FIRSTROW=2) 
WITH (
    Name varchar(100),
    Latitude float,
    Longitude float,
    Address varchar(max),
    Icon varchar(100),
) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("Name", reader.GetName(0));
                    Assert.AreEqual("Latitude", reader.GetName(1));
                    Assert.AreEqual("Longitude", reader.GetName(2));
                    Assert.AreEqual("Address", reader.GetName(3));
                    Assert.AreEqual("Icon", reader.GetName(4));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Statue of Liberty", reader.GetString(0));
                    Assert.AreEqual(40.689247f, reader.GetFloat(1));
                    Assert.AreEqual(-74.044502f, reader.GetFloat(2));
                    Assert.AreEqual("Liberty Island, New York, NY 10004", reader.GetString(3));
                    Assert.AreEqual("\\icons\\sol.png", reader.GetString(4));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ReadCsvLastRow()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $@"
SELECT * FROM OPENROWSET(BULK '{path}', FORMAT='CSV', LASTROW=1) 
WITH (
    Name varchar(100),
    Latitude float,
    Longitude float,
    Address varchar(max),
    Icon varchar(100),
) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("Name", reader.GetName(0));
                    Assert.AreEqual("Latitude", reader.GetName(1));
                    Assert.AreEqual("Longitude", reader.GetName(2));
                    Assert.AreEqual("Address", reader.GetName(3));
                    Assert.AreEqual("Icon", reader.GetName(4));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Empire State Building", reader.GetString(0));
                    Assert.AreEqual(40.748817f, reader.GetFloat(1));
                    Assert.AreEqual(-73.985428f, reader.GetFloat(2));
                    Assert.AreEqual("20 W 34th St, New York, NY 10118", reader.GetString(3));
                    Assert.AreEqual("\\icons\\sol.png", reader.GetString(4));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ReadSingleBLOB()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK '{path}', SINGLE_BLOB) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("BulkColumn", reader.GetName(0));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    var expected = File.ReadAllBytes(path);
                    var actual = new byte[expected.Length];
                    var read = 0L;
                    while (read < expected.Length)
                    {
                        var newRead = reader.GetBytes(0, read, actual, (int)read, actual.Length - (int)read);
                        Assert.AreNotEqual(0, newRead, "GetBytes should return 0 only when there is no more data to read");
                        read += newRead;
                    }

                    // Check that we have read all data
                    Assert.AreEqual(0, reader.GetBytes(0, read, actual, 0, 1));
                    CollectionAssert.AreEqual(expected, actual);

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ReadSingleCLOB()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK '{path}', SINGLE_CLOB) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("BulkColumn", reader.GetName(0));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(File.ReadAllText(path), reader.GetString(0));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ReadSingleNCLOB()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "OPENROWSET.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK '{path}', SINGLE_NCLOB)  AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    // Check column names
                    Assert.AreEqual("value", reader.GetName(0));

                    // Check data
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(File.ReadAllText(path), reader.GetString(0));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ErrorReadingSingleBLOBWithWildcard()
        {
            var path = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "Resources", "*.csv");
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM OPENROWSET(BULK '{path}', SINGLE_NCLOB) AS t";
                using (var reader = cmd.ExecuteReader())
                {
                    var ex = Assert.ThrowsException<Sql4CdsException>(() => reader.Read());
                    Assert.AreEqual(4860, ex.Number);
                }
            }
        }
    }
}
