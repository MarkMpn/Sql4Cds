using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class RowCountEstimate
    {
        public RowCountEstimate(int value)
        {
            Value = value;
        }

        public int Value { get; }
    }

    class RowCountEstimateDefiniteRange : RowCountEstimate
    {
        public RowCountEstimateDefiniteRange(int min, int max) : base(max)
        {
            Minimum = min;
            Maximum = max;
        }

        public int Minimum { get; }
        public int Maximum { get; }

        public static RowCountEstimateDefiniteRange ExactlyOne { get; } = new RowCountEstimateDefiniteRange(1, 1);

        public static RowCountEstimateDefiniteRange ZeroOrOne { get; } = new RowCountEstimateDefiniteRange(0, 1);
    }
}
