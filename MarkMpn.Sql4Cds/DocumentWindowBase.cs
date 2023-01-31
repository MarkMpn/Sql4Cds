using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MarkMpn.Sql4Cds
{
    public abstract class DocumentWindowBase : WeifenLuo.WinFormsUI.Docking.DockContent, IDocumentWindow
    {
        private bool _modified;
        private string _displayName;
        private string _filename;
        private DateTime? _lastModified;

        public bool Modified
        {
            get { return _modified; }
            set
            {
                if (_modified == value)
                    return;

                _modified = value;
                SyncTitle();
            }
        }

        public string DisplayName
        {
            get { return _displayName; }
            set
            {
                _displayName = value;
                SyncTitle();
            }
        }

        public string Filename
        {
            get { return _filename; }
            set
            {
                _filename = value;

                if (!String.IsNullOrEmpty(value))
                    DisplayName = Path.GetFileName(value);
            }
        }

        protected void SyncTitle()
        {
            if (InvokeRequired)
            {
                Invoke(new Action(SyncTitle));
                return;
            }

            Text = GetTitle();
        }

        protected virtual string GetTitle()
        {
            return DisplayName + (Modified ? " *" : "");
        }

        public TabContent GetSessionDetails()
        {
            return new TabContent
            {
                Type = Type,
                Query = Modified ? Content : null,
                Filename = Filename
            };
        }

        public void RestoreSessionDetails(TabContent tab)
        {
            if (!String.IsNullOrEmpty(tab.Filename) && this is ISaveableDocumentWindow saveable)
                saveable.Open(tab.Filename);
            else
                Filename = tab.Filename;

            if (tab.Query != null)
                Content = tab.Query;
        }

        protected abstract string Type { get; }

        public abstract string Content { get; set; }

        public void Open(string filename)
        {
            Filename = filename;
            Content = File.ReadAllText(filename);
            Modified = false;

            try
            {
                _lastModified = new FileInfo(filename).LastWriteTimeUtc;
            }
            catch
            {
            }
        }

        public void Save(string filename)
        {
            Filename = filename;
            File.WriteAllText(Filename, Content);
            Modified = false;
            _lastModified = new FileInfo(Filename).LastWriteTimeUtc;
        }

        protected void CheckForNewVersion(object sender, EventArgs e)
        {
            if (_lastModified != null)
            {
                try
                {
                    var latestModified = new FileInfo(Filename).LastWriteTimeUtc;
                    if (latestModified > _lastModified)
                    {
                        if (MessageBox.Show(this, "This file has been modified by another program.\r\nDo you want to reload it?" + (_modified ? "\r\n\r\nYour local changes will be lost." : ""), "Reload", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2) == DialogResult.Yes)
                        {
                            Content = File.ReadAllText(Filename);
                            Modified = false;
                        }
                        else
                        {
                            Modified = true;
                        }

                        _lastModified = latestModified;
                        SyncTitle();
                    }
                }
                catch
                {
                }
            }
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            base.OnClosing(e);

            if (Modified && this is ISaveableDocumentWindow saveable)
            {
                using (var form = new ConfirmCloseForm(new[] { DisplayName }, true))
                {
                    switch (form.ShowDialog())
                    {
                        case DialogResult.Yes:
                            var filename = Filename;

                            if (filename == null)
                            {
                                using (var save = new SaveFileDialog())
                                {
                                    save.Filter = saveable.Filter;

                                    if (save.ShowDialog(this) != DialogResult.OK)
                                    {
                                        e.Cancel = true;
                                        return;
                                    }

                                    filename = save.FileName;
                                }
                            }

                            saveable.Save(filename);
                            break;

                        case DialogResult.Cancel:
                            e.Cancel = true;
                            break;
                    }
                }
            }
        }
    }
}
