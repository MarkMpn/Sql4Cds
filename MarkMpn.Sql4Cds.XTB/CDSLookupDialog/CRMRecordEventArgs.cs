using Microsoft.Xrm.Sdk;
using System;

namespace xrmtb.XrmToolBox.Controls
{
    public class CRMRecordEventArgs : EventArgs
    {
        public CRMRecordEventArgs(int columnIndex, int rowIndex, Entity entity, string attribute)
        {
            ColumnIndex = columnIndex;
            RowIndex = rowIndex;
            Entity = entity;
            Attribute = attribute;
        }

        public int ColumnIndex { get; }

        public int RowIndex { get; }

        public Entity Entity { get; }

        public string Attribute { get; }

        public object Value { get { return Entity != null && Entity.Contains(Attribute) ? Entity[Attribute] : null; } }

        public void OnRecordEvent(object sender, CRMRecordEventHandler RecordEventHandler)
        {
            RecordEventHandler?.Invoke(sender, this);
        }
    }

    public delegate void CRMRecordEventHandler(object sender, CRMRecordEventArgs e);
}
