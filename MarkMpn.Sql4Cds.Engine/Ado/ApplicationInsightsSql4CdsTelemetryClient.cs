using System;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;

namespace MarkMpn.Sql4Cds.Engine
{
    internal sealed class ApplicationInsightsSql4CdsTelemetryClient : ISql4CdsTelemetryClient
    {
        private readonly TelemetryClient _client;

        private ApplicationInsightsSql4CdsTelemetryClient(TelemetryClient client)
        {
            _client = client;
        }

        public static ISql4CdsTelemetryClient Create()
        {
            return new ApplicationInsightsSql4CdsTelemetryClient(new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration
            {
                ConnectionString = "InstrumentationKey=79761278-a908-4575-afbf-2f4d82560da6"
            }));
        }

        public void TrackCommandEvent(string eventName, string queryType, string source)
        {
            var telemetry = new EventTelemetry(eventName);
            telemetry.Properties["QueryType"] = queryType;
            telemetry.Properties["Source"] = source;
            _client.TrackEvent(telemetry);
        }

        public void TrackException(Exception exception, string sql, string source, string errorNumber)
        {
            var telemetry = new ExceptionTelemetry(exception);
            telemetry.Properties["Sql"] = sql;
            telemetry.Properties["Source"] = source;

            if (!String.IsNullOrEmpty(errorNumber))
                telemetry.Properties["ErrorNumber"] = errorNumber;

            _client.TrackException(telemetry);
        }
    }
}
