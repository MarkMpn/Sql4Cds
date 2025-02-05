using System;
using System.Drawing;

namespace MarkMpn.Sql4Cds.XTB
{
    static class IconColorizer
    {
        /// <summary>
        /// Converts the color of an icon to indicate a particular highlight color
        /// </summary>
        /// <param name="icon">The icon to colorize</param>
        /// <param name="color">The color to apply</param>
        /// <param name="referenceColorPoint">The point within the icon of the color to change from</param>
        /// <param name="bounds">The area of the icon to colorize</param>
        /// <returns>The converted icon</returns>
        public static Icon ApplyEnvironmentHighlightColor(Icon icon, Color color, Point referenceColorPoint, Rectangle bounds)
        {
            var bitmap = icon.ToBitmap();

            ApplyEnvironmentHighlightColor(bitmap, color, referenceColorPoint, bounds);

            return Icon.FromHandle(bitmap.GetHicon());
        }

        /// <summary>
        /// Converts the color of an icon to indicate a particular highlight color
        /// </summary>
        /// <param name="icon">The icon to colorize</param>
        /// <param name="color">The color to apply</param>
        /// <param name="referenceColorPoint">The point within the icon of the color to change from</param>
        /// <param name="bounds">The area of the icon to colorize</param>
        public static void ApplyEnvironmentHighlightColor(Bitmap bitmap, Color color, Point referenceColorPoint, Rectangle bounds)
        {
            // Adjust the standard SQL icon to use the selected color
            var referenceColor = bitmap.GetPixel(referenceColorPoint.X, referenceColorPoint.Y);

            // Convert the bitmap to the requested hue. Only adjust the database part of the icon, not the document part
            for (var x = bounds.Left; x < bounds.Right; x++)
            {
                for (var y = bounds.Top; y < bounds.Bottom; y++)
                {
                    var srcColor = bitmap.GetPixel(x, y);
                    var hue = srcColor.GetHue();
                    var sat = srcColor.GetSaturation();
                    var lum = srcColor.GetBrightness();

                    hue = hue + color.GetHue() - referenceColor.GetHue();
                    if (hue < 0)
                        hue += 360;
                    sat = sat + color.GetSaturation() - referenceColor.GetSaturation();
                    if (sat < 0)
                        sat = 0;
                    else if (sat > 1)
                        sat = 1;
                    lum = lum + color.GetBrightness() - referenceColor.GetBrightness();
                    if (lum < 0)
                        lum = 0;
                    else if (lum > 1)
                        lum = 1;

                    var newColor = HSL2RGB(srcColor.A, hue / 360f, sat, lum);

                    bitmap.SetPixel(x, y, newColor);
                }
            }
        }

        // https://geekymonkey.com/Programming/CSharp/RGB2HSL_HSL2RGB.htm
        private static Color HSL2RGB(int alpha, double h, double sl, double l)
        {
            double v;
            double r, g, b;

            r = l;   // default to gray
            g = l;
            b = l;
            v = (l <= 0.5) ? (l * (1.0 + sl)) : (l + sl - l * sl);

            if (v > 0)
            {
                double m;
                double sv;
                int sextant;
                double fract, vsf, mid1, mid2;

                m = l + l - v;
                sv = (v - m) / v;
                h *= 6.0;
                sextant = (int)h;
                fract = h - sextant;
                vsf = v * sv * fract;
                mid1 = m + vsf;
                mid2 = v - vsf;

                switch (sextant)
                {
                    case 0:
                        r = v;
                        g = mid1;
                        b = m;
                        break;

                    case 1:
                        r = mid2;
                        g = v;
                        b = m;
                        break;

                    case 2:
                        r = m;
                        g = v;
                        b = mid1;
                        break;

                    case 3:
                        r = m;
                        g = mid2;
                        b = v;
                        break;

                    case 4:
                        r = mid1;
                        g = m;
                        b = v;
                        break;

                    case 5:
                        r = v;
                        g = m;
                        b = mid2;
                        break;
                }
            }

            var rgb = Color.FromArgb(alpha, Convert.ToByte(r * 255.0f), Convert.ToByte(g * 255.0f), Convert.ToByte(b * 255.0f));
            return rgb;
        }
    }
}
