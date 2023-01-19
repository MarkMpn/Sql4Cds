namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// Object metadata information
    /// </summary>
    public class ObjectMetadata
    {
        public MetadataType MetadataType { get; set; }

        public string MetadataTypeName { get; set; }

        public string Schema { get; set; }

        public string Name { get; set; }

        public string ParentName { get; set; }

        public string ParentTypeName { get; set; }

        public string Urn { get; set; }
    }
}
