using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace AutocompleteMenuNS
{
    /// <summary>
    /// Wrapper over the control like TextBox.
    /// </summary>
    public interface ITextBoxWrapper
    {
        Control TargetControl { get; }
        string Text { get; }
        string SelectedText { get; set; }
        int SelectionLength { get; set; }
        int SelectionStart { get; set; }
        Point GetPositionFromCharIndex(int pos);
        bool Readonly { get; }
        event EventHandler LostFocus;
        event ScrollEventHandler Scroll;
        event KeyEventHandler KeyDown;
        event MouseEventHandler MouseDown;
    }
}
