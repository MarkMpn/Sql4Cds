namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// The type of object suggested by an <see cref="Autocomplete.SqlAutocompleteItem"/>
    /// </summary>
    public enum AutocompleteItemKind
    {
        Instance,
        Schema,
        Table,
        TableValuedFunction,
        StoredProcedure,
        StoredProcedureParameter,
        OneToManyJoin,
        ManyToOneJoin,
        Attribute,
        VirtualAttribute,
        Function,
        AggregateFunction,
        OptionSetValue,
        EntityName,
        Collation,
        Variable,
    }
}
