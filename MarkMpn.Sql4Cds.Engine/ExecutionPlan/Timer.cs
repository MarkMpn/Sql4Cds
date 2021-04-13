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
        private DateTime? _startTime;
        private TimeSpan _duration;

        public Timer()
        {
            _startTime = DateTime.Now;
        }

        public TimeSpan Duration => _duration;

        public void Pause()
        {
            var endTime = DateTime.Now;

            if (_startTime != null)
                _duration += (endTime - _startTime.Value);

            _startTime = null;
        }

        public void Resume()
        {
            _startTime = DateTime.Now;
        }
    }
}
