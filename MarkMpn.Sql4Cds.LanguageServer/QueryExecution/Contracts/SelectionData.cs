using System;
using System.Diagnostics;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary> 
    /// Container class for a selection range from file 
    /// </summary>
    /// TODO: Remove this in favor of buffer range end-to-end
    public class SelectionData
    {
        public SelectionData() { }

        public SelectionData(int startLine, int startColumn, int endLine, int endColumn)
        {
            StartLine = startLine;
            StartColumn = startColumn;
            EndLine = endLine;
            EndColumn = endColumn;
        }

        #region Properties

        public int EndColumn { get; set; }

        public int EndLine { get; set; }

        public int StartColumn { get; set; }
        public int StartLine { get; set; }

        #endregion

        public BufferRange ToBufferRange()
        {
            return new BufferRange(StartLine, StartColumn, EndLine, EndColumn);
        }

        public static SelectionData FromBufferRange(BufferRange range)
        {
            return new SelectionData
            {
                StartLine = range.Start.Line,
                StartColumn = range.Start.Column,
                EndLine = range.End.Line,
                EndColumn = range.End.Column
            };
        }


        /// <summary>
        /// Provides details about a range between two positions in
        /// a file buffer.
        /// </summary>
        [DebuggerDisplay("Start = {Start.Line}:{Start.Column}, End = {End.Line}:{End.Column}")]
        public class BufferRange
        {
            #region Properties

            /// <summary>
            /// Provides an instance that represents a range that has not been set.
            /// </summary>
            public static readonly BufferRange None = new BufferRange(0, 0, 0, 0);

            /// <summary>
            /// Gets the start position of the range in the buffer.
            /// </summary>
            public BufferPosition Start { get; private set; }

            /// <summary>
            /// Gets the end position of the range in the buffer.
            /// </summary>
            public BufferPosition End { get; private set; }

            /// <summary>
            /// Returns true if the current range is non-zero, i.e.
            /// contains valid start and end positions.
            /// </summary>
            public bool HasRange
            {
                get
                {
                    return Equals(None);
                }
            }

            #endregion

            #region Constructors

            /// <summary>
            /// Creates a new instance of the BufferRange class.
            /// </summary>
            /// <param name="start">The start position of the range.</param>
            /// <param name="end">The end position of the range.</param>
            public BufferRange(BufferPosition start, BufferPosition end)
            {
                if (start > end)
                {
                    throw new ArgumentException();
                }

                Start = start;
                End = end;
            }

            /// <summary>
            /// Creates a new instance of the BufferRange class.
            /// </summary>
            /// <param name="startLine">The 1-based starting line number of the range.</param>
            /// <param name="startColumn">The 1-based starting column number of the range.</param>
            /// <param name="endLine">The 1-based ending line number of the range.</param>
            /// <param name="endColumn">The 1-based ending column number of the range.</param>
            public BufferRange(
                int startLine,
                int startColumn,
                int endLine,
                int endColumn)
            {
                Start = new BufferPosition(startLine, startColumn);
                End = new BufferPosition(endLine, endColumn);
            }

            #endregion

            #region Public Methods

            /// <summary>
            /// Compares two instances of the BufferRange class.
            /// </summary>
            /// <param name="obj">The object to which this instance will be compared.</param>
            /// <returns>True if the ranges are equal, false otherwise.</returns>
            public override bool Equals(object obj)
            {
                if (!(obj is BufferRange))
                {
                    return false;
                }

                BufferRange other = (BufferRange)obj;

                return
                    Start.Equals(other.Start) &&
                    End.Equals(other.End);
            }

            /// <summary>
            /// Calculates a unique hash code that represents this instance.
            /// </summary>
            /// <returns>A hash code representing this instance.</returns>
            public override int GetHashCode()
            {
                return Start.GetHashCode() ^ End.GetHashCode();
            }

            #endregion
        }


        /// <summary>
        /// Provides details about a position in a file buffer.  All
        /// positions are expressed in 1-based positions (i.e. the
        /// first line and column in the file is position 1,1).
        /// </summary>
        [DebuggerDisplay("Position = {Line}:{Column}")]
        public class BufferPosition
        {
            #region Properties

            /// <summary>
            /// Provides an instance that represents a position that has not been set.
            /// </summary>
            public static readonly BufferPosition None = new BufferPosition(-1, -1);

            /// <summary>
            /// Gets the line number of the position in the buffer.
            /// </summary>
            public int Line { get; private set; }

            /// <summary>
            /// Gets the column number of the position in the buffer.
            /// </summary>
            public int Column { get; private set; }

            #endregion

            #region Constructors

            /// <summary>
            /// Creates a new instance of the BufferPosition class.
            /// </summary>
            /// <param name="line">The line number of the position.</param>
            /// <param name="column">The column number of the position.</param>
            public BufferPosition(int line, int column)
            {
                Line = line;
                Column = column;
            }

            #endregion

            #region Public Methods

            /// <summary>
            /// Compares two instances of the BufferPosition class.
            /// </summary>
            /// <param name="obj">The object to which this instance will be compared.</param>
            /// <returns>True if the positions are equal, false otherwise.</returns>
            public override bool Equals(object obj)
            {
                if (!(obj is BufferPosition))
                {
                    return false;
                }

                BufferPosition other = (BufferPosition)obj;

                return
                    Line == other.Line &&
                    Column == other.Column;
            }

            /// <summary>
            /// Calculates a unique hash code that represents this instance.
            /// </summary>
            /// <returns>A hash code representing this instance.</returns>
            public override int GetHashCode()
            {
                return Line.GetHashCode() ^ Column.GetHashCode();
            }

            /// <summary>
            /// Compares two positions to check if one is greater than the other.
            /// </summary>
            /// <param name="positionOne">The first position to compare.</param>
            /// <param name="positionTwo">The second position to compare.</param>
            /// <returns>True if positionOne is greater than positionTwo.</returns>
            public static bool operator >(BufferPosition positionOne, BufferPosition positionTwo)
            {
                return
                    positionOne != null && positionTwo == null ||
                    positionOne.Line > positionTwo.Line ||
                    positionOne.Line == positionTwo.Line &&
                     positionOne.Column > positionTwo.Column;
            }

            /// <summary>
            /// Compares two positions to check if one is less than the other.
            /// </summary>
            /// <param name="positionOne">The first position to compare.</param>
            /// <param name="positionTwo">The second position to compare.</param>
            /// <returns>True if positionOne is less than positionTwo.</returns>
            public static bool operator <(BufferPosition positionOne, BufferPosition positionTwo)
            {
                return positionTwo > positionOne;
            }

            #endregion
        }
    }
}
