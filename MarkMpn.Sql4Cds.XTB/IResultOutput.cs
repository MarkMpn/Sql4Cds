namespace MarkMpn.Sql4Cds.XTB
{
    interface IResultOutput
    {
        void AddRow(object[] values);

        void Update();

        void Complete();
    }
}
