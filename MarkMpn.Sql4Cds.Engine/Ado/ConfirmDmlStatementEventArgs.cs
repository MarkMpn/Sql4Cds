using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Text;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Contains information about a DML statement (INSERT/UPDATE/DELETE) that is about to be executed
    /// </summary>
    public class ConfirmDmlStatementEventArgs : CancelEventArgs
    {
        /// <summary>
        /// Creates a new <see cref="ConfirmDmlStatementEventArgs"/>
        /// </summary>
        /// <param name="count">The number of records that will be affected</param>
        /// <param name="metadata">The metadata of the entity that will be affected</param>
        /// <param name="bypassCustomPluginExecution">Indicates if custom plugins will be bypassed by the operation</param>
        internal ConfirmDmlStatementEventArgs(int count, EntityMetadata metadata, bool bypassCustomPluginExecution)
        {
            Count = count;
            Metadata = metadata;
            BypassCustomPluginExecution = bypassCustomPluginExecution;
        }

        /// <summary>
        /// Creates a new <see cref="ConfirmDmlStatementEventArgs"/>
        /// </summary>
        /// <param name="count">The number of records that will be affected</param>
        /// <param name="dataTable">The details of the temporary table that will be affected</param>
        /// <param name="bypassCustomPluginExecution">Indicates if custom plugins will be bypassed by the operation</param>
        internal ConfirmDmlStatementEventArgs(int count, DataTable dataTable, bool bypassCustomPluginExecution)
        {
            Count = count;
            DataTable = dataTable;
            BypassCustomPluginExecution = bypassCustomPluginExecution;
        }

        /// <summary>
        /// Returns the number of records that will be affected by the operation
        /// </summary>
        public int Count { get; }

        /// <summary>
        /// Returns the metadata of the entity that will be affected
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Returns the details of the temporary table that will be affected
        /// </summary>
        public DataTable DataTable { get; }

        /// <summary>
        /// Indicates if custom plugins will be bypassed by the operation
        /// </summary>
        public bool BypassCustomPluginExecution { get; }
    }
}
