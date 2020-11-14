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

        /// <summary>
        /// Gets the metadata for an entity if it's already available in the cache
        /// </summary>
        /// <param name="logicalName">The logical name of the entity to get the metadata for</param>
        /// <param name="metadata">The cached view of the metadata for the requested entity</param>
        /// <returns><c>true</c> if the metadata was available in the cache, or <c>false</c> otherwise</returns>
        /// <remarks>
        /// If the data is not available in the cache, this method will return <c>false</c> and the <paramref name="metadata"/>
        /// parameter will be set to <c>null</c>. It will also start a background task to load the metadata so it may be available
        /// on later attempts
        /// </remarks>
        bool TryGetValue(string logicalName, out EntityMetadata metadata);

        /// <summary>
        /// Gets a minimal amount of metadata for an entity if it's already available in the cache
        /// </summary>
        /// <param name="logicalName">The logical name of the entity to get the metadata for</param>
        /// <param name="metadata">The cached view of the metadata for the requested entity</param>
        /// <returns><c>true</c> if the metadata was available in the cache, or <c>false</c> otherwise</returns>
        /// <remarks>
        /// If the data is not available in the cache, this method will return <c>false</c> and the <paramref name="metadata"/>
        /// parameter will be set to <c>null</c>. It will also start a background task to load the metadata so it may be available
        /// on later attempts
        /// 
        /// The metadata retrieved by this method is only required to include the following details:
        /// <ul>
        ///     <li>LogicalName</li>
        ///     <li>
        ///         Attributes
        ///         <ul>
        ///             <li>LogicalName</li>
        ///             <li>AttributeOf</li>
        ///             <li>AttributeType</li>
        ///             <li>DisplayName</li>
        ///             <li>Description</li>
        ///             <li>IsValidForUpdate</li>
        ///         </ul>
        ///     </li>
        ///     <li>
        ///         OneToManyRelationships &amp; ManyToOneRelationships
        ///         <ul>
        ///             <li>ReferencedEntity</li>
        ///             <li>ReferencingEntity</li>
        ///             <li>ReferencedAttribute</li>
        ///             <li>ReferencingAttribute</li>
        ///         </ul>
        ///     </li>
        /// </ul>
        /// </remarks>
        bool TryGetMinimalData(string logicalName, out EntityMetadata metadata);
    }
}
