using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Reflection;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Describes a collation to be used to compare strings
    /// </summary>
    class Collation
    {
        private static Dictionary<string, int> _collationNameToLcid;

        static Collation()
        {
            _collationNameToLcid = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("MarkMpn.Sql4Cds.Engine.resources.CollationNameToLCID.txt"))
            using (var reader = new StreamReader(stream))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split('\t');
                    _collationNameToLcid[parts[0]] = Int32.Parse(parts[1]);
                }
            }
        }

        /// <summary>
        /// Creates a collation using the locale ID and additional comparison options
        /// </summary>
        /// <param name="name">The name the collation was parsed from</param>
        /// <param name="lcid">The locale ID to use</param>
        /// <param name="compareOptions">Additional comparison options</param>
        public Collation(string name, int lcid, SqlCompareOptions compareOptions)
        {
            Name = name;
            LCID = lcid;
            CompareOptions = compareOptions;
        }

        /// <summary>
        /// Creates a collation using the locale ID and common additional options
        /// </summary>
        /// <param name="lcid">The locale ID to use</param>
        /// <param name="caseSensitive">Indicates if comparisons are case sensitive</param>
        /// <param name="accentSensitive">Indicates if comparisons are accent sensitive</param>
        public Collation(int lcid, bool caseSensitive, bool accentSensitive)
        {
            var compareOptions = SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth;

            if (!caseSensitive)
                compareOptions |= SqlCompareOptions.IgnoreCase;

            if (!accentSensitive)
                compareOptions |= SqlCompareOptions.IgnoreNonSpace;

            LCID = lcid;
            CompareOptions = compareOptions;
        }

        /// <summary>
        /// Returns the locale ID to use when comparing strings
        /// </summary>
        public int LCID { get; }

        /// <summary>
        /// Returns the additional options to use when comparing strings
        /// </summary>
        public SqlCompareOptions CompareOptions { get; }

        /// <summary>
        /// Returns the name of the collation
        /// </summary>
        /// <remarks>
        /// This will be null for the default collation
        /// </remarks>
        public string Name { get; }

        /// <summary>
        /// Returns the default collation to be used for system data
        /// </summary>
        public static Collation USEnglish { get; } = new Collation(1033, false, false);

        /// <summary>
        /// Attempts to parse the name of a collation to the corresponding details
        /// </summary>
        /// <param name="name">The name of the collation to parse</param>
        /// <param name="coll">The details of the collation parsed from the <paramref name="name"/></param>
        /// <returns><c>true</c> if the <paramref name="name"/> could be parsed, or <c>false</c> otherwise</returns>
        public static bool TryParse(string name, out Collation coll)
        {
            var compareOptions = SqlCompareOptions.IgnoreKanaType | SqlCompareOptions.IgnoreWidth;
            var parts = name.Split('_');
            var @as = false;
            var cs = false;

            for (var i = parts.Length - 1; i >= 0; i--)
            {
                switch (parts[i].ToUpperInvariant())
                {
                    case "BIN":
                        compareOptions |= SqlCompareOptions.BinarySort;
                        break;

                    case "BIN2":
                        compareOptions |= SqlCompareOptions.BinarySort2;
                        break;

                    case "CI":
                        compareOptions |= SqlCompareOptions.IgnoreCase;
                        break;

                    case "CS":
                        cs = true;
                        break;

                    case "AI":
                        compareOptions |= SqlCompareOptions.IgnoreNonSpace;
                        break;

                    case "AS":
                        @as = true;
                        break;

                    case "KS":
                        compareOptions &= ~SqlCompareOptions.IgnoreKanaType;
                        break;

                    case "WS":
                        compareOptions &= ~SqlCompareOptions.IgnoreWidth;
                        break;

                    case "UTF8":
                        break;

                    default:
                        // Check we've got sufficient and non-contradictory information
                        if ((compareOptions.HasFlag(SqlCompareOptions.BinarySort) || compareOptions.HasFlag(SqlCompareOptions.BinarySort2)))
                        {
                            // If BIN or BIN2 are set, other options shouldn't be set
                            if (i < parts.Length - 2)
                                break;
                        }
                        else
                        {
                            // Must specify case sensitivity
                            if (!compareOptions.HasFlag(SqlCompareOptions.IgnoreCase) && !cs)
                                break;

                            // Must specify accent sensitivity
                            if (!compareOptions.HasFlag(SqlCompareOptions.IgnoreNonSpace) && !@as)
                                break;

                            // Can't be both CS and CI
                            if (compareOptions.HasFlag(SqlCompareOptions.IgnoreCase) && cs)
                                break;

                            // Can't be both AS and AI
                            if (compareOptions.HasFlag(SqlCompareOptions.IgnoreNonSpace) && @as)
                                break;
                        }

                        var collationName = String.Join("_", parts, 0, i + 1);

                        if (!_collationNameToLcid.TryGetValue(collationName, out var lcid))
                            break;
                        
                        coll = new Collation(name, lcid, compareOptions);
                        return true;
                }
            }

            coll = null;
            return false;
        }

        /// <summary>
        /// Applies the current collation to a string value
        /// </summary>
        /// <param name="value">The string value to apply the collation to</param>
        /// <returns>A new <see cref="SqlString"/> value with the given string <paramref name="value"/> and the current collation</returns>
        public SqlString ToSqlString(string value)
        {
            return new SqlString(value, LCID, CompareOptions);
        }
    }
}
