using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides similar methods to the <see cref="Parallel"/> class, but allows the loop to indicate that the number of threads
    /// should change during execution.
    /// </summary>
    class DynamicParallel<TSource, TLocal>
    {
        class DynamicParallelThreadState
        {
            public Task Task { get; set; }

            public DynamicParallelVote? Vote { get; set; }

            public bool IsStopped { get; set; }

            public CancellationToken CancellationToken { get; set; }
        }

        private readonly ConcurrentQueue<TSource> _queue;
        private readonly List<DynamicParallelThreadState> _threads;
        private readonly DynamicParallelLoopState _loopState;
        private readonly ConcurrentBag<Exception> _exceptions;
        private readonly ParallelOptions _parallelOptions;
        private readonly Func<TLocal> _localInit;
        private readonly Func<TSource, DynamicParallelLoopState, TLocal, Task<DynamicParallelVote?>> _body;
        private readonly Func<TLocal, Task> _localFinally;

        public DynamicParallel(
            IEnumerable<TSource> source,
            ParallelOptions parallelOptions,
            Func<TLocal> localInit,
            Func<TSource, DynamicParallelLoopState, TLocal, Task<DynamicParallelVote?>> body,
            Func<TLocal, Task> localFinally)
        {
            _queue = new ConcurrentQueue<TSource>(source);
            _threads = new List<DynamicParallelThreadState>();
            _loopState = new DynamicParallelLoopState();
            _exceptions = new ConcurrentBag<Exception>();
            _parallelOptions = parallelOptions;
            _localInit = localInit;
            _body = body;
            _localFinally = localFinally;
        }

        public void ForEach(CancellationToken cancellationToken)
        {
            StartThread(cancellationToken);

            var monitor = Task.Factory.StartNew(async () => await MonitorAsync(cancellationToken)).Unwrap();
            monitor.Wait();

            if (!_exceptions.IsEmpty)
                throw new AggregateException(_exceptions);
        }

        private void StartThread(CancellationToken cancellationToken)
        {
            var threadState = new DynamicParallelThreadState
            {
                CancellationToken = cancellationToken
            };
            threadState.Task = Task.Factory.StartNew((state) => RunThread((DynamicParallelThreadState)state), threadState).Unwrap();
            _threads.Add(threadState);
        }

        private async Task RunThread(DynamicParallelThreadState threadState)
        {
            var local = _localInit();

            try
            {
                while (!_loopState.IsStopped &&
                    !threadState.IsStopped &&
                    _queue.TryDequeue(out var item))
                {
                    threadState.Vote = await _body(item, _loopState, local) ?? threadState.Vote;
                }
            }
            catch (Exception ex)
            {
                _exceptions.Add(ex);
                _loopState.Stop();
            }
            finally
            {
                await _localFinally(local);
                threadState.Vote = null;
            }
        }

        private async Task MonitorAsync(CancellationToken cancellationToken)
        {
            var hasDecreased = false;

            // Periodically check the votes and adjust the number of threads
            while (!_loopState.IsStopped)
            {
                var allTasks = Task.WhenAll(_threads.Select(t => t.Task).ToArray());
                var timeout = Task.Delay(1000, cancellationToken);
                var task = await Task.WhenAny(allTasks, timeout);

                if (task == allTasks)
                    break;

                var votes = _threads.Where(t => t.Vote != null && !t.IsStopped).Select(t => t.Vote.Value).ToList();
                var increase = votes.Count(v => v == DynamicParallelVote.IncreaseThreads);
                var decrease = votes.Count(v => v == DynamicParallelVote.DecreaseThreads);

                if (decrease > 0 && _threads.Count(t => !t.IsStopped) > 1)
                {
                    var thread = _threads.First(t => !t.IsStopped && t.Vote == DynamicParallelVote.DecreaseThreads);
                    thread.IsStopped = true;
                    hasDecreased = true;
                }
                else if (increase > 0 && !hasDecreased && _threads.Count(t => !t.IsStopped) < _parallelOptions.MaxDegreeOfParallelism)
                {
                    StartThread(cancellationToken);
                }

                foreach (var thread in _threads)
                    thread.Vote = null;

            }
        }
    }

    class DynamicParallelLoopState
    {
        public bool IsStopped { get; private set; }

        public void Stop()
        {
            IsStopped = true;
        }
    }

    enum DynamicParallelVote
    {
        IncreaseThreads,
        DecreaseThreads,
    }
}
