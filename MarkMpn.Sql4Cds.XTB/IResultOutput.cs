namespace MarkMpn.Sql4Cds.XTB
{
    interface IResultOutput
    {
        void AddRow(object[] values, object[] providerSpecificValues);

        void Update();

        void Complete();
    }
}
