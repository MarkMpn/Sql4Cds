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
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Controls
{
    public class ExecutionPlanView : ScrollableControl
    {
        class Line
        {
            public IExecutionPlanNode Source { get; set; }
            public Point Start { get; set; }
            public Point End { get; set; }
            public int Width { get; set; }

            public Rectangle MBR => new Rectangle(End.X, End.Y - Width / 2, Start.X - End.X + 1, Start.Y - End.Y + Width / 2 + 1);
        }

        private IExecutionPlanNode _plan;
        private IDictionary<IExecutionPlanNode, Rectangle> _nodeLocations;
        private List<Line> _lines;
        private int _maxY;
        private int _maxBottom;
        private ToolTip _toolTip;
        private IExecutionPlanNode _tooltipNode;

        const int _offset = 32;
        private readonly Size _iconSize = new Size(32, 32);

        public ExecutionPlanView()
        {
            AutoScroll = true;
            DoubleBuffered = true;

            _toolTip = new ToolTip
            {
                InitialDelay = 1000, // Hover the mouse for 1 second before showing the tooltip
                AutoPopDelay = 5000, // Show the tooltip for 5 seconds once triggered
                ReshowDelay = 1000,  // Delay 1 second between different controls
            };
        }

        public bool Executed { get; set; }

        public IDictionary<string, DataSource> DataSources { get; set; }

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

        public QueryExecutionException Exception { get; set; }

        private void LayoutPlan()
        {
            _nodeLocations = new Dictionary<IExecutionPlanNode, Rectangle>
            {
                [_plan] = new Rectangle(new Point(_offset, _offset), MeasureNode(_plan))
            };
            _lines = new List<Line>();

            _maxY = _offset;
            _maxBottom = _nodeLocations[_plan].Bottom;

            LayoutChildren(_plan);

            AutoScrollMinSize = new Size(_nodeLocations.Max(kvp => kvp.Value.Right + _offset), _maxBottom + _offset);
        }

        private void LayoutChildren(IExecutionPlanNode parent)
        {
            var i = 0;

            var sourceCount = parent.GetSources().Count();
            var parentRect = _nodeLocations[parent];
            var parentIconRect = GetIconRect(parentRect);
            var lineYSpacing = _iconSize.Height / (sourceCount + 1);

            foreach (var child in parent.GetSources())
            {
                var size = MeasureNode(child);

                Point pt;

                if (i == 0)
                {
                    // First source stays on the same level as the parent, one step to the right
                    pt = new Point(parentRect.Right + _offset, parentRect.Top);
                }
                else
                {
                    // Subsequent nodes move down
                    pt = new Point(parentRect.Right + _offset, _maxBottom + _offset);
                    _maxY = pt.Y;
                    _maxBottom = _maxY + size.Height;
                }

                var fullRect = new Rectangle(pt, size);
                var iconRect = GetIconRect(fullRect);

                _nodeLocations[child] = fullRect;

                var rows = child is IDataExecutionPlanNode dataChild ? Executed ? dataChild.RowsOut : dataChild.EstimatedRowsOut : 1;
                var width = rows == 0 ? 1 : (int)Math.Log10(rows);
                _lines.Add(new Line { Source = child, Start = new Point(iconRect.Left, parentIconRect.Top == iconRect.Top ? (parentIconRect.Top + (i + 1) * lineYSpacing) : (iconRect.Top + iconRect.Height / 2)), End = new Point(parentIconRect.Right, parentIconRect.Top + (i + 1) * lineYSpacing), Width = width });

                LayoutChildren(child);
                i++;
            }
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            base.OnPaint(e);

            // Apply the scrolling transform so we can still work with original coordinates for each node even when scrolled
            e.Graphics.TranslateTransform(AutoScrollPosition.X, AutoScrollPosition.Y);
            var clipRect = e.ClipRectangle;
            clipRect.Offset(-AutoScrollPosition.X, -AutoScrollPosition.Y);

            // Inflate the clip rectangle slightly to account for antialiasing - removes scrolling artifacts
            clipRect.Inflate(1, 1);

            // Clear the background
            e.Graphics.SmoothingMode = SmoothingMode.AntiAlias;
            e.Graphics.FillRectangle(SystemBrushes.Window, clipRect);

            // Draw each node within the clip rectangle
            foreach (var kvp in _nodeLocations)
            {
                if (!kvp.Value.IntersectsWith(clipRect))
                    continue;

                var iconRect = GetIconRect(kvp.Value);

                if (iconRect.IntersectsWith(clipRect))
                {
                    var imageName = kvp.Key.GetType().Name;

                    if (imageName == "AdaptiveIndexSpoolNode")
                        imageName = "IndexSpoolNode";
                    else if (imageName == "OpenJsonNode" || imageName == "SystemFunctionNode")
                        imageName = "ExecuteMessageNode";

                    using (var stream = GetType().Assembly.GetManifestResourceStream(GetType(), "Images." + imageName + ".ico"))
                    {
                        Image image;

                        if (stream != null)
                        {
                            image = Bitmap.FromStream(stream);
                        }
                        else
                        {
                            using (var fallbackStream = GetType().Assembly.GetManifestResourceStream(GetType(), "Images.FallbackNode.ico"))
                            {
                                image = Bitmap.FromStream(fallbackStream);
                            }
                        }

                        e.Graphics.DrawImage(image, iconRect);
                    }

                    if (Exception?.Node == kvp.Key || (kvp.Key is IExecutionPlanNodeWarning warning && warning.Warning != null))
                    {
                        // Show an error icon overlay
                        e.Graphics.DrawImage(SystemIcons.Error.ToBitmap(), new Rectangle(iconRect.Location, new Size(16, 16)));
                    }
                }

                var labelRect = new Rectangle(kvp.Value.X, iconRect.Bottom, kvp.Value.Width, kvp.Value.Height - iconRect.Height);

                if (labelRect.IntersectsWith(clipRect))
                {
                    var label = GetLabel(kvp.Key);
                    var stringFormat = new StringFormat { Alignment = StringAlignment.Center };

                    if (Focused && kvp.Key == Selected)
                    {
                        e.Graphics.FillRectangle(SystemBrushes.Highlight, labelRect);
                        e.Graphics.DrawString(label, Font, SystemBrushes.HighlightText, labelRect, stringFormat);
                    }
                    else
                    {
                        e.Graphics.DrawString(label, Font, SystemBrushes.ControlText, labelRect, stringFormat);
                    }
                }
            }

            // Draw each edge within the clip rectangle
            foreach (var line in _lines)
            {
                if (!line.MBR.IntersectsWith(clipRect))
                    continue;

                using (var pen = new Pen(Color.Gray, line.Width))
                {
                    if (line.Start.Y == line.End.Y)
                    {
                        // Draw a straight, horizontal line
                        e.Graphics.DrawLine(pen, line.Start.X, line.Start.Y, line.End.X + line.Width, line.End.Y);
                    }
                    else
                    {
                        // Draw the line with a dogleg
                        var midX = line.Start.X - (line.Start.X - line.End.X) / 2;
                        e.Graphics.DrawLine(pen, line.Start.X, line.Start.Y, midX, line.Start.Y);
                        e.Graphics.DrawLine(pen, midX, line.Start.Y + line.Width / 2, midX, line.End.Y - line.Width / 2);
                        e.Graphics.DrawLine(pen, midX, line.End.Y, line.End.X + line.Width, line.End.Y);
                    }
                }

                // Draw the arrowhead
                e.Graphics.FillPolygon(Brushes.Gray, new[]
                {
                    line.End,
                    new Point(line.End.X + line.Width + 2, line.End.Y - (line.Width / 2) - 2),
                    new Point(line.End.X + line.Width + 2, line.End.Y + (line.Width / 2) + 2)
                });
            }
        }

        private Size MeasureNode(IExecutionPlanNode node)
        {
            var label = GetLabel(node);

            using (var bitmap = new Bitmap(100, 100))
            using (var g = Graphics.FromImage(bitmap))
            {
                var labelSize = g.MeasureString(label, Font);

                const int iconLabelGap = 10;

                var width = Math.Max(_iconSize.Width, labelSize.Width);
                return new Size((int) Math.Ceiling(width), _iconSize.Height + iconLabelGap + (int) Math.Ceiling(labelSize.Height));
            }
        }

        private Rectangle GetIconRect(Rectangle fullRect)
        {
            return new Rectangle(fullRect.X + (fullRect.Width - _iconSize.Width) / 2, fullRect.Y, _iconSize.Width, _iconSize.Height);
        }

        private string GetLabel(IExecutionPlanNode node)
        {
            var text = node.ToString();

            if (Executed)
            {
                var nodeTime = node.Duration.TotalMilliseconds - node.GetSources().Sum(source => source.Duration.TotalMilliseconds);
                var totalTime = Plan.Duration.TotalMilliseconds;

                var cost = Math.Min(1, Math.Max(0, nodeTime / totalTime));

                if (Double.IsNaN(cost))
                    cost = 1;

                text += "\r\nCost: " + cost.ToString("P0");
                text += "\r\n" + (nodeTime / 1000).ToString("N3") + "s";
            }

            return text;
        }

        public IExecutionPlanNode Selected { get; private set; }

        public event EventHandler NodeSelected;

        protected void OnNodeSelected(EventArgs e)
        {
            NodeSelected?.Invoke(this, e);
        }

        private Rectangle OriginToClient(Rectangle rect)
        {
            rect.Offset(AutoScrollPosition.X, AutoScrollPosition.Y);
            return rect;
        }

        protected override void OnMouseClick(MouseEventArgs e)
        {
            base.OnMouseClick(e);
            Focus();

            var hit = HitTest(e.Location);

            if (Selected != null)
                Invalidate(Selected);

            Selected = hit;

            if (Selected != null)
                Invalidate(Selected);

            OnNodeSelected(EventArgs.Empty);
        }

        protected override void OnMouseMove(MouseEventArgs e)
        {
            base.OnMouseMove(e);

            var hit = HitTest(e.Location);

            if (hit != _tooltipNode)
            {
                if (Exception != null && Exception.Node == hit)
                    _toolTip.SetToolTip(this, Exception.Message);
                else if (hit is IExecutionPlanNodeWarning warning && warning.Warning != null)
                    _toolTip.SetToolTip(this, warning.Warning);
                else if (hit is IFetchXmlExecutionPlanNode fetch)
                    _toolTip.SetToolTip(this, fetch.FetchXmlString);
                else
                    _toolTip.SetToolTip(this, null);

                _tooltipNode = hit;
            }
        }

        private IExecutionPlanNode HitTest(Point pt)
        {
            foreach (var node in _nodeLocations)
            {
                if (OriginToClient(node.Value).Contains(pt))
                    return node.Key;
            }

            return null;
        }

        protected override void OnGotFocus(EventArgs e)
        {
            base.OnGotFocus(e);

            if (Selected != null)
                Invalidate(Selected);
        }

        protected override void OnLostFocus(EventArgs e)
        {
            base.OnLostFocus(e);

            if (Selected != null)
                Invalidate(Selected);
        }

        private void Invalidate(IExecutionPlanNode node)
        {
            // Inflate the node rectangle by 1 pixel to allow for antialiasing
            var rect = OriginToClient(_nodeLocations[node]);
            rect.Inflate(1, 1);
            Invalidate(rect);
        }
    }
}
