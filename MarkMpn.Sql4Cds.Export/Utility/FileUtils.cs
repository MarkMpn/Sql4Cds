//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.IO;

namespace Microsoft.SqlTools.ServiceLayer.Utility
{
    internal static class FileUtilities
    {
        /// <summary>
        /// Deletes a file and swallows exceptions, if any
        /// </summary>
        /// <param name="path"></param>
        internal static void SafeFileDelete(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch (Exception)
            {
                // Swallow exception, do nothing
            }
        }

        internal static int WriteWithLength(Stream stream, byte[] buffer, int length)
        {
            stream.Write(buffer, 0, length);
            return length;
        }
    }
}