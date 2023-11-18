using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using ScintillaNET;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class FindReplace : UserControl
    {
        private readonly ToolStripItem[] _row1;
        private readonly ToolStripItem[] _row2;
        private readonly Scintilla _editor;

        public FindReplace(Scintilla editor)
        {
            InitializeComponent();

            _row1 = new ToolStripItem[]
            {
                showReplaceToolStripButton,
                findToolStripComboBox,
                findToolStripSplitButton,
                closeToolStripButton
            };
            _row2 = new ToolStripItem[]
            {
                replaceSpacerToolStripLabel,
                replaceToolStripComboBox,
                replaceToolStripButton,
                replaceAllToolStripButton
            };
            _editor = editor;

            ShowReplace = false;

            OnSizeChanged(EventArgs.Empty);
        }

        public bool ShowReplace
        {
            get { return _row2.All(c => c.Visible); }
            set
            {
                showReplaceToolStripButton.Checked = value;

                foreach (var c in _row2)
                    c.Visible = value;

                ClientSize = new Size(ClientSize.Width, toolStrip1.Height + panel1.Height);
            }
        }

        public void ShowFind()
        {
            findToolStripComboBox.Focus();
        }

        protected override void OnSizeChanged(EventArgs e)
        {
            base.OnSizeChanged(e);

            StretchComboBox(_row1);
            StretchComboBox(_row2);
        }

        private void StretchComboBox(ToolStripItem[] row)
        {
            if (row == null)
                return;

            var comboBox = row.OfType<ToolStripComboBox>().Single();
            comboBox.Width = ClientSize.Width - row.Except(new[] { comboBox }).Sum(c => c.Width) - 5;
        }

        private void showReplaceToolStripButton_Click(object sender, EventArgs e)
        {
            ShowReplace = showReplaceToolStripButton.Checked;
        }

        protected override bool ProcessCmdKey(ref Message msg, Keys keyData)
        {
            if (keyData == Keys.Escape)
            {
                Hide();
                return true;
            }

            if (keyData == Keys.Tab && findToolStripComboBox.Focused && ShowReplace)
            {
                replaceToolStripComboBox.Focus();
                return true;
            }

            if (keyData == (Keys.Tab | Keys.Shift) && replaceToolStripComboBox.Focused)
            {
                findToolStripComboBox.Focus();
                return true;
            }

            if (keyData == Keys.Tab && replaceToolStripComboBox.Focused)
            {
                toolStrip1.Focus();
                findToolStripSplitButton.Select();
                return true;
            }

            if (keyData == (Keys.Tab | Keys.Shift) && toolStrip1.Focused && findToolStripSplitButton.Selected && ShowReplace)
            {
                replaceToolStripComboBox.Focus();
                return true;
            }

            if (keyData == Keys.Tab && closeToolStripButton.Selected)
            {
                if (ShowReplace)
                    replaceToolStripButton.Select();
                else
                    matchCaseToolStripButton.Select();

                return true;
            }

            if (keyData == (Keys.Tab | Keys.Shift) && toolStrip1.Focused && ((ShowReplace && replaceToolStripButton.Selected) || (!ShowReplace && matchCaseToolStripButton.Selected)))
            {
                closeToolStripButton.Select();
                return true;
            }

            if (keyData == (Keys.Alt | Keys.R) && ShowReplace)
            {
                Replace(false);
                return true;
            }

            if (keyData == (Keys.Alt | Keys.A) && ShowReplace)
            {
                Replace(true);
                return true;
            }

            return base.ProcessCmdKey(ref msg, keyData);
        }

        private void closeToolStripButton_Click(object sender, EventArgs e)
        {
            Hide();
        }

        private void findToolStripComboBox_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == '\r')
                findNextToolStripMenuItem.PerformClick();
        }

        private void findToolStripSplitButton_ButtonClick(object sender, EventArgs e)
        {
            FindNext();
        }

        public void FindNext()
        {
            AddHistory(findToolStripComboBox);

            _editor.SearchFlags = SearchFlags.None;

            if (matchCaseToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.MatchCase;

            if (wholeWordToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.WholeWord;

            if (regexToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.Regex;

            _editor.TargetStart = _editor.SelectionStart + 1;
            _editor.TargetEnd = _editor.TextLength;

            while (true)
            {
                var match = _editor.SearchInTarget(findToolStripComboBox.Text);

                if (match != -1)
                {
                    _editor.SetSelection(_editor.TargetStart, _editor.TargetEnd);
                    _editor.ScrollCaret();
                    break;
                }
                else if (_editor.TargetStart > 0)
                {
                    _editor.TargetStart = 0;
                }
                else
                {
                    break;
                }
            }
        }

        private void findPreviousToolStripMenuItem_Click(object sender, EventArgs e)
        {
            FindPrevious();
        }

        public void FindPrevious()
        {
            AddHistory(findToolStripComboBox);

            _editor.SearchFlags = SearchFlags.None;

            if (matchCaseToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.MatchCase;

            if (wholeWordToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.WholeWord;

            if (regexToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.Regex;

            _editor.TargetStart = _editor.SelectionStart - 1;
            _editor.TargetEnd = 0;

            while (true)
            {
                var match = _editor.SearchInTarget(findToolStripComboBox.Text);

                if (match != -1)
                {
                    _editor.SetSelection(_editor.TargetStart, _editor.TargetEnd);
                    _editor.ScrollCaret();
                    break;
                }
                else if (_editor.TargetStart < _editor.TextLength)
                {
                    _editor.TargetStart = _editor.TextLength;
                    _editor.TargetEnd = 0;
                }
                else
                {
                    break;
                }
            }
        }

        private void AddHistory(ToolStripComboBox comboBox)
        {
            if (!comboBox.Items.Contains(comboBox.Text))
                comboBox.Items.Insert(0, comboBox.Text);

            while (comboBox.Items.Count > 5)
                comboBox.Items.RemoveAt(5);
        }

        private void replaceToolStripButton_Click(object sender, EventArgs e)
        {
            Replace(false);
        }

        private void replaceAllToolStripButton_Click(object sender, EventArgs e)
        {
            Replace(true);
        }

        public void Replace(bool all)
        {
            AddHistory(findToolStripComboBox);
            AddHistory(replaceToolStripComboBox);

            _editor.BeginUndoAction();

            _editor.SearchFlags = SearchFlags.None;

            if (matchCaseToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.MatchCase;

            if (wholeWordToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.WholeWord;

            if (regexToolStripButton.Checked)
                _editor.SearchFlags |= SearchFlags.Regex;

            _editor.TargetStart = all ? 0 : _editor.SelectionStart + 1;
            _editor.TargetEnd = _editor.TextLength;

            var replacements = 0;
            var lastMatch = -1;
            var lastReplacementLength = 0;

            while (true)
            {
                var match = _editor.SearchInTarget(findToolStripComboBox.Text);

                if (match != -1)
                {
                    lastMatch = match;

                    if (regexToolStripButton.Checked)
                        lastReplacementLength = _editor.ReplaceTargetRe(replaceToolStripComboBox.Text);
                    else
                        lastReplacementLength = _editor.ReplaceTarget(replaceToolStripComboBox.Text);

                    if (!all)
                        break;

                    replacements++;

                    _editor.TargetStart = _editor.TargetEnd;
                    _editor.TargetEnd = _editor.TextLength;
                }
                else if (_editor.TargetStart > 0 && !all)
                {
                    _editor.TargetStart = 0;
                }
                else
                {
                    break;
                }
            }

            _editor.EndUndoAction();

            if (replacements > 0)
            {
                _editor.SetSelection(lastMatch, lastMatch + lastReplacementLength);
                _editor.ScrollCaret();
            }

            if (all)
                MessageBox.Show($"{replacements} occurrence(s) replaced.", "Replace All", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
    }
}
