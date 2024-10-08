using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Holds context for a session that should be persisted across multiple queries
    /// </summary>
    class SessionContext
    {
        /// <summary>
        /// Provides just-in-time access to global variables that require a service request to determine the value for
        /// </summary>
        class SessionContextVariables : IDictionary<string, INullable>
        {
            private readonly SessionContext _context;
            private readonly Dictionary<string, Lazy<INullable>> _values;

            public SessionContextVariables(SessionContext context)
            {
                _context = context;
                _values = new Dictionary<string, Lazy<INullable>>(StringComparer.OrdinalIgnoreCase);
                Reset();
            }

            public void Reset()
            {
                _values["@@SERVERNAME"] = new Lazy<INullable>(() => GetServerName());
                _values["@@VERSION"] = new Lazy<INullable>(() => GetVersion());
            }

            private SqlString GetVersion()
            {
                var dataSource = _context.DataSources[_context._options.PrimaryDataSource];
                string orgVersion = null;

#if NETCOREAPP
                if (dataSource.Connection is ServiceClient svc)
                    orgVersion = svc.ConnectedOrgVersion.ToString();
#else
                if (dataSource.Connection is CrmServiceClient svc)
                    orgVersion = svc.ConnectedOrgVersion.ToString();
#endif

                if (orgVersion == null)
                    orgVersion = ((RetrieveVersionResponse)dataSource.Execute(new RetrieveVersionRequest())).Version;

                var assembly = typeof(Sql4CdsConnection).Assembly;
                var assemblyVersion = assembly.GetName().Version;
                var assemblyCopyright = assembly
                    .GetCustomAttributes(typeof(AssemblyCopyrightAttribute), false)
                    .OfType<AssemblyCopyrightAttribute>()
                    .FirstOrDefault()?
                    .Copyright;
                var assemblyFilename = assembly.Location;
                var assemblyDate = System.IO.File.GetLastWriteTime(assemblyFilename);

                return $"Microsoft Dataverse - {orgVersion}\r\n\tSQL 4 CDS - {assemblyVersion}\r\n\t{assemblyDate:MMM dd yyyy HH:mm:ss}\r\n\t{assemblyCopyright}";
            }

            private SqlString GetServerName()
            {
                var dataSource = _context.DataSources[_context._options.PrimaryDataSource];

#if NETCOREAPP
                var svc = dataSource.Connection as ServiceClient;

                if (svc != null)
                    return svc.ConnectedOrgUriActual.Host;
#else
                var svc = dataSource.Connection as CrmServiceClient;

                if (svc != null)
                    return svc.CrmConnectOrgUriActual.Host;
#endif

                return dataSource.Name;
            }

            public INullable this[string key]
            {
                get => _values[key].Value;
                set => throw new NotImplementedException();
            }

            public ICollection<string> Keys => _values.Keys;

            public ICollection<INullable> Values => _values.Values.Select(v => v.Value).ToArray();

            public int Count => _values.Count;

            public bool IsReadOnly => true;

            public void Add(string key, INullable value)
            {
                throw new NotImplementedException();
            }

            public void Add(KeyValuePair<string, INullable> item)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(KeyValuePair<string, INullable> item)
            {
                return _values.TryGetValue(item.Key, out var val)
                    && val.Value.Equals(item.Value);
            }

            public bool ContainsKey(string key)
            {
                return _values.ContainsKey(key);
            }

            public void CopyTo(KeyValuePair<string, INullable>[] array, int arrayIndex)
            {
                foreach (var item in this)
                {
                    array[arrayIndex] = item;
                    arrayIndex++;
                }
            }

            public IEnumerator<KeyValuePair<string, INullable>> GetEnumerator()
            {
                foreach (var item in _values)
                    yield return new KeyValuePair<string, INullable>(item.Key, item.Value.Value);
            }

            public bool Remove(string key)
            {
                throw new NotImplementedException();
            }

            public bool Remove(KeyValuePair<string, INullable> item)
            {
                throw new NotImplementedException();
            }

            public bool TryGetValue(string key, out INullable value)
            {
                if (!_values.TryGetValue(key, out var lazy))
                {
                    value = default;
                    return false;
                }

                value = lazy.Value;
                return true;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private readonly IQueryExecutionOptions _options;
        private readonly SessionContextVariables _variables;

        public SessionContext(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options)
        {
            _options = options;
            DataSources = dataSources;
            DateFormat = DateFormat.mdy;
            _variables = new SessionContextVariables(this);

            GlobalVariableTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase)
            {
                ["@@IDENTITY"] = DataTypeHelpers.EntityReference,
                ["@@ROWCOUNT"] = DataTypeHelpers.Int,
                ["@@ERROR"] = DataTypeHelpers.Int,
            };

            GlobalVariableValues = new LayeredDictionary<string, INullable>(
                new Dictionary<string, INullable>(StringComparer.OrdinalIgnoreCase)
                {
                    ["@@IDENTITY"] = SqlEntityReference.Null,
                    ["@@ROWCOUNT"] = (SqlInt32)0,
                    ["@@ERROR"] = (SqlInt32)0,
                },
                _variables);

            GetServerDetails();
            _options.PrimaryDataSourceChanged += (_, __) => GetServerDetails();
        }

        private void GetServerDetails()
        {
            GlobalVariableTypes["@@SERVERNAME"] = DataTypeHelpers.NVarChar(100, DataSources[_options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault);
            GlobalVariableTypes["@@VERSION"] = DataTypeHelpers.NVarChar(Int32.MaxValue, DataSources[_options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault);
            _variables.Reset();
        }

        /// <summary>
        /// Returns the data sources that are available to the query
        /// </summary>
        public IDictionary<string, DataSource> DataSources { get; }

        /// <summary>
        /// Returns or sets the current SET DATEFORMAT option
        /// </summary>
        public DateFormat DateFormat { get; set; }

        /// <summary>
        /// Returns the types of the global variables
        /// </summary>
        internal Dictionary<string, DataTypeReference> GlobalVariableTypes { get; }

        /// <summary>
        /// Returns the values of the global variables
        /// </summary>
        internal IDictionary<string, INullable> GlobalVariableValues { get; }
    }
}
