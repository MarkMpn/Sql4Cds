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
        /// <param name="description">A description of the collation for display purposes</param>
        public Collation(string name, int lcid, SqlCompareOptions compareOptions, string description)
        {
            Name = name;
            LCID = lcid;
            CompareOptions = compareOptions;
            Description = description;
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
        /// Returns the description of the collation
        /// </summary>
        public string Description { get; }

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
                        
                        coll = new Collation(name, lcid, compareOptions, null);
                        return true;
                }
            }

            coll = null;
            return false;
        }

        public static IEnumerable<Collation> GetAllCollations()
        {
            foreach (var kvp in _collationNameToLcid)
            {
                yield return new Collation(kvp.Key + "_BIN", kvp.Value, SqlCompareOptions.BinarySort, kvp.Key + ", binary sort");
                yield return new Collation(kvp.Key + "_BIN2", kvp.Value, SqlCompareOptions.BinarySort2, kvp.Key + ", binary code point comparison sort");

                var options = SqlCompareOptions.None;
                var description = new string[5];
                description[0] = kvp.Key;

                foreach (var c in new[] { "_CI", "_CS" })
                {
                    if (c == "_CI")
                    {
                        options |= SqlCompareOptions.IgnoreCase;
                        description[1] = "case-insensitive";
                    }
                    else
                    {
                        options &= ~SqlCompareOptions.IgnoreCase;
                        description[1] = "case-sensitive";
                    }

                    foreach (var a in new[] { "_AI", "_AS" })
                    {
                        if (a == "_AI")
                        {
                            options |= SqlCompareOptions.IgnoreNonSpace;
                            description[2] = "accent-insensitive";
                        }
                        else
                        {
                            options &= ~SqlCompareOptions.IgnoreNonSpace;
                            description[2] = "accent-sensitive";
                        }

                        foreach (var k in new[] { "", "_KS" })
                        {
                            if (k == "")
                            {
                                options |= SqlCompareOptions.IgnoreKanaType;
                                description[3] = "kanatype-insensitive";
                            }
                            else
                            {
                                options &= ~SqlCompareOptions.IgnoreKanaType;
                                description[3] = "kanatype-sensitive";
                            }

                            foreach (var w in new[] { "", "_WS" })
                            {
                                if (w == "")
                                {
                                    options |= SqlCompareOptions.IgnoreWidth;
                                    description[4] = "width-insensitive";
                                }
                                else
                                {
                                    options &= ~SqlCompareOptions.IgnoreWidth;
                                    description[4] = "width-sensitive";
                                }

                                // Albanian-100, case-sensitive, accent-insensitive, kanatype-sensitive, width-insensitive
                                yield return new Collation(kvp.Key + c + a + k + w, kvp.Value, options, String.Join(", ", description));
                            }
                        }
                    }
                }
            }
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
