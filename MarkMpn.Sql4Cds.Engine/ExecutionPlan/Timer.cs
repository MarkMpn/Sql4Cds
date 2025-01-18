using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides a simple timer for measuring the execution time of a node
    /// </summary>
    class Timer
    {
        private TimeSpan _duration;

        public Timer()
        {
        }

        public TimeSpan Duration => _duration;

        public IDisposable Run()
        {
            return new TimedRegion(this);
        }

        private class TimedRegion : IDisposable
        {
            private readonly Timer _timer;
            private readonly DateTime _startTime;

            public TimedRegion(Timer timer)
            {
                _timer = timer;
                _startTime = DateTime.UtcNow;
            }

            public void Dispose()
            {
                _timer._duration += DateTime.UtcNow - _startTime;
            }
        }
    }
}
