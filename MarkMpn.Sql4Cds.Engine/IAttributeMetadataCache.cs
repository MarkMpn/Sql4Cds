using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides metadata for SQL &lt;-&gt; FetchXML conversion
    /// </summary>
    /// <remarks>
    /// Use the <see cref="AttributeMetadataCache"/> for a standard implementation, or provide your own if you already
    /// have a source of metadata available to improve the performance of query conversion
    /// </remarks>
    public interface IAttributeMetadataCache
    {
        /// <summary>
        /// Retrieves the metadata for an entity
        /// </summary>
        /// <param name="name">The logical name of the entity to get the metadata for</param>
        /// <returns>The metadata for the requested entity</returns>
        EntityMetadata this[string name] { get; }

        bool TryGetValue(string logicalName, out EntityMetadata metadata);
    }
}
