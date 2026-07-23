using System;

namespace MarkMpn.Sql4Cds.Engine
{
    internal interface ISql4CdsTelemetryClient
    {
        void TrackCommandEvent(string eventName, string queryType, string source);
        void TrackException(Exception exception, string sql, string source, string errorNumber);
    }

    internal sealed class NullSql4CdsTelemetryClient : ISql4CdsTelemetryClient
    {
        public static readonly ISql4CdsTelemetryClient Instance = new NullSql4CdsTelemetryClient();

        private NullSql4CdsTelemetryClient()
        {
        }

        public void TrackCommandEvent(string eventName, string queryType, string source)
        {
        }

        public void TrackException(Exception exception, string sql, string source, string errorNumber)
        {
        }
    }
}
