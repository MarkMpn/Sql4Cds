using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class ConfirmCloseForm : Form
    {
        public ConfirmCloseForm(string[] files, bool cancelable)
        {
            InitializeComponent();

            listBox.Items.Clear();
            listBox.Items.AddRange(files);

            cancelButton.Enabled = cancelable;
        }

        protected override void OnKeyDown(KeyEventArgs e)
        {
            base.OnKeyDown(e);

            if (e.KeyCode == Keys.Y)
                DialogResult = DialogResult.Yes;
            else if (e.KeyCode == Keys.N)
                DialogResult = DialogResult.No;
        }
    }
}
