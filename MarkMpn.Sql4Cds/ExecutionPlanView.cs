using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds
{
    public class ExecutionPlanView : ScrollableControl
    {
        class Line
        {
            public Point Start { get; set; }
            public Point End { get; set; }

            public Rectangle MBR => new Rectangle(End.X, End.Y, Start.X - End.X + 1, Start.Y - End.Y + 1);
        }

        private IExecutionPlanNode _plan;
        private IDictionary<IExecutionPlanNode, Rectangle> _nodeLocations;
        private List<Line> _lines;
        private int _maxY;

        const int _offset = 32;
        private readonly Size _size = new Size(32, 32);

        public ExecutionPlanView()
        {
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.UserPaint, true);
        }

        public IExecutionPlanNode Plan
        {
            get { return _plan; }
            set
            {
                _plan = value;
                LayoutPlan();
                Invalidate();
            }
        }

        private void LayoutPlan()
        {
            _nodeLocations = new Dictionary<IExecutionPlanNode, Rectangle>
            {
                [_plan] = new Rectangle(new Point(_offset, _offset), _size)
            };
            _lines = new List<Line>();

            _maxY = _offset;

            LayoutChildren(_plan);

            ClientSize = new Size(_nodeLocations.Max(kvp => kvp.Value.Right + _offset), _maxY + _size.Height + _offset);
        }

        private void LayoutChildren(IExecutionPlanNode parent)
        {
            var i = 0;

            var sourceCount = parent.GetSources().Count();
            var parentRect = _nodeLocations[parent];
            var lineYSpacing = _size.Height / (sourceCount + 1);

            foreach (var child in parent.GetSources())
            {
                Point pt;

                if (i == 0)
                {
                    // First source stays on the same level as the parent, one step to the right
                    pt = new Point(parentRect.Right + _offset, parentRect.Top);
                }
                else
                {
                    // Subsequent nodes move down
                    pt = new Point(parentRect.Right + _offset, _maxY + _size.Height + _offset);
                    _maxY = pt.Y;
                }

                _nodeLocations[child] = new Rectangle(pt, _size);
                _lines.Add(new Line { Start = new Point(pt.X, pt.Y + _size.Height / 2), End = new Point(parentRect.Right, parentRect.Top + (i + 1) * lineYSpacing) });

                LayoutChildren(child);
                i++;
            }
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            base.OnPaint(e);

            e.Graphics.SmoothingMode = SmoothingMode.AntiAlias;
            e.Graphics.FillRectangle(SystemBrushes.Window, e.ClipRectangle);

            foreach (var kvp in _nodeLocations)
            {
                if (!kvp.Value.IntersectsWith(e.ClipRectangle))
                    continue;

                e.Graphics.FillRectangle(Brushes.Red, kvp.Value);
                e.Graphics.DrawString(kvp.Key.GetType().Name, Font, SystemBrushes.ControlText, kvp.Value);
            }

            foreach (var line in _lines)
            {
                if (!line.MBR.IntersectsWith(e.ClipRectangle))
                    continue;

                var midX = line.Start.X - (line.Start.X - line.End.X) / 2;
                e.Graphics.DrawLine(Pens.Black, line.Start.X, line.Start.Y, midX, line.Start.Y);
                e.Graphics.DrawLine(Pens.Black, midX, line.Start.Y, midX, line.End.Y);
                e.Graphics.DrawLine(Pens.Black, midX, line.End.Y, line.End.X, line.End.Y);
            }
        }
    }
}
