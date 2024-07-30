using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ColorCode.Styling;
using ColorCode;
using Markdig;
using Markdig.Parsers;
using Markdig.Syntax;
using Markdig.Helpers;
using Markdig.Renderers;
using Markdig.Renderers.Html;

namespace MarkMpn.Sql4Cds.XTB
{
    /// <summary>
    /// Adapted from Markdown.ColorCode with specific extensions for copying code to the SQL editor
    /// and .NET Framework compatibility
    /// </summary>
    static class MarkdownPipelineBuilderExtensions
    {
        internal sealed class LanguageExtractor
        {
            private readonly string _defaultLanguageId;

            /// <summary>
            ///     Create a new <see cref="LanguageExtractor"/> with the specified <paramref name="additionalLanguages"/> and <paramref name="defaultLanguageId"/>.
            /// </summary>
            /// <param name="additionalLanguages">Additional languages used to augment the built-in languages provided by ColorCode-Universal.</param>
            /// <param name="defaultLanguageId">The default language ID. Used when the language ID cannot be extracted from the <see cref="FencedCodeBlock"/> or if the language was not found.</param>
            public LanguageExtractor(IEnumerable<ILanguage> additionalLanguages, string defaultLanguageId)
            {
                foreach (var language in additionalLanguages)
                {
                    Languages.Load(language);
                }

                _defaultLanguageId = defaultLanguageId;
            }

            /// <inheritdoc />
            public ILanguage ExtractLanguage(IFencedBlock fencedBlock, FencedCodeBlockParser fencedCodeBlockParser)
            {
                var languageId = fencedBlock.Info.Replace(fencedCodeBlockParser.InfoPrefix, string.Empty);
                var language = string.IsNullOrWhiteSpace(languageId) ? null : Languages.FindById(languageId);

                if (language is null && !string.IsNullOrWhiteSpace(_defaultLanguageId))
                {
                    return Languages.FindById(_defaultLanguageId);
                }

                return language;
            }
        }

        internal sealed class CodeExtractor
        {
            /// <inheritdoc />
            public string ExtractCode(LeafBlock leafBlock)
            {
                var code = new StringBuilder();

                // ReSharper disable once NullCoalescingConditionIsAlwaysNotNullAccordingToAPIContract
                var lines = leafBlock.Lines.Lines ?? Array.Empty<StringLine>();
                var totalLines = lines.Length;

                for (var index = 0; index < totalLines; index++)
                {
                    var line = lines[index];
                    var slice = line.Slice;

                    if (slice.Text == null)
                    {
                        continue;
                    }

                    var lineText = slice.Text.Substring(slice.Start, slice.Length);

                    if (index > 0)
                    {
                        code.AppendLine();
                    }

                    code.Append(lineText);
                }

                return code.ToString();
            }
        }

        /// <summary>
        ///     A Markdig extension which colorizes code using ColorCode.
        /// </summary>
        internal sealed class ColorCodeExtension : IMarkdownExtension
        {
            private readonly LanguageExtractor _languageExtractor;

            private readonly CodeExtractor _codeExtractor;

            private readonly HtmlFormatter _htmlFormatter;

            /// <summary>
            ///     Create a new <see cref="ColorCodeExtension"/>.
            /// </summary>
            /// <param name="languageExtractor">The <see cref="ILanguageExtractor"/> to use with the extension.</param>
            /// <param name="codeExtractor">The <see cref="ICodeExtractor"/> to use with the extension.</param>
            /// <param name="htmlFormatter">The <see cref="IHtmlFormatter"/> to use with the extension.</param>
            public ColorCodeExtension(
                LanguageExtractor languageExtractor,
                CodeExtractor codeExtractor,
                HtmlFormatter htmlFormatter)
            {
                _languageExtractor = languageExtractor;
                _codeExtractor = codeExtractor;
                _htmlFormatter = htmlFormatter;
            }

            /// <summary>
            ///     Sets up this extension for the specified pipeline.
            /// </summary>
            /// <param name="pipeline">The pipeline.</param>
            public void Setup(MarkdownPipelineBuilder pipeline)
            {
            }

            /// <summary>
            ///     Sets up this extension for the specified renderer.
            /// </summary>
            /// <param name="pipeline">The pipeline used to parse the document.</param>
            /// <param name="renderer">The renderer.</param>
            public void Setup(MarkdownPipeline pipeline, IMarkdownRenderer renderer)
            {
                if (!(renderer is TextRendererBase<HtmlRenderer> htmlRenderer))
                {
                    return;
                }

                var codeBlockRenderer = htmlRenderer.ObjectRenderers.FindExact<CodeBlockRenderer>();

                if (!(codeBlockRenderer is null))
                {
                    htmlRenderer.ObjectRenderers.Remove(codeBlockRenderer);
                }
                else
                {
                    codeBlockRenderer = new CodeBlockRenderer();
                }

                htmlRenderer.ObjectRenderers.AddIfNotAlready(
                    new ColorCodeBlockRenderer(
                        codeBlockRenderer,
                        _languageExtractor,
                        _codeExtractor,
                        _htmlFormatter
                    )
                );
            }
        }


        /// <summary>
        ///     A renderer which colorizes code blocks using ColorCode.
        /// </summary>
        internal sealed class ColorCodeBlockRenderer : HtmlObjectRenderer<CodeBlock>
        {
            private readonly CodeBlockRenderer _underlyingCodeBlockRenderer;
            private readonly LanguageExtractor _languageExtractor;
            private readonly CodeExtractor _codeExtractor;
            private readonly HtmlFormatter _htmlFormatter;

            /// <summary>
            ///     Create a new <see cref="ColorCodeBlockRenderer"/>.
            /// </summary>
            /// <param name="underlyingCodeBlockRenderer">The underlying CodeBlockRenderer to handle unsupported languages.</param>
            /// <param name="languageExtractor"> A <see cref="ILanguageExtractor"/> used to extract the <see cref="ILanguage"/> from the <see cref="CodeBlock"/>.</param>
            /// <param name="codeExtractor">A <see cref="ICodeExtractor"/> used to extract the code from the <see cref="CodeBlock"/>.</param>
            /// <param name="htmlFormatter">A <see cref="IHtmlFormatter"/> for generating HTML strings.</param>
            public ColorCodeBlockRenderer(
                CodeBlockRenderer underlyingCodeBlockRenderer,
                LanguageExtractor languageExtractor,
                CodeExtractor codeExtractor,
                HtmlFormatter htmlFormatter)
            {
                _underlyingCodeBlockRenderer = underlyingCodeBlockRenderer;
                _languageExtractor = languageExtractor;
                _codeExtractor = codeExtractor;
                _htmlFormatter = htmlFormatter;
            }

            /// <summary>
            ///     Writes the specified <paramref name="codeBlock"/> to the <paramref name="renderer"/>.
            /// </summary>
            /// <param name="renderer">The renderer.</param>
            /// <param name="codeBlock">The code block to render.</param>
            protected override void Write(HtmlRenderer renderer, CodeBlock codeBlock)
            {
                if (!(codeBlock is FencedCodeBlock fencedCodeBlock) ||
                    !(codeBlock.Parser is FencedCodeBlockParser fencedCodeBlockParser))
                {
                    _underlyingCodeBlockRenderer.Write(renderer, codeBlock);

                    return;
                }

                var language = _languageExtractor.ExtractLanguage(fencedCodeBlock, fencedCodeBlockParser);

                if (language is null)
                {
                    _underlyingCodeBlockRenderer.Write(renderer, codeBlock);

                    return;
                }

                var code = _codeExtractor.ExtractCode(codeBlock);
                var html = _htmlFormatter.GetHtmlString(code, language);

                // Add copy button
                var divEnd = html.IndexOf(">") + 1;
                html = html.Insert(divEnd, "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 2048 2048\" class=\"copyCode\" focusable=\"false\"><title>Copy Code</title><path d=\"M1920 805v1243H640v-384H128V0h859l384 384h128l421 421zm-384-37h165l-165-165v165zM640 384h549L933 128H256v1408h384V384zm1152 512h-384V512H768v1408h1024V896z\"></path></svg>");

                renderer.Write(html);
            }
        }

        public static MarkdownPipelineBuilder UseSqlCodeBlockHandling(
            this MarkdownPipelineBuilder markdownPipelineBuilder,
            StyleDictionary styleDictionary = null,
            IEnumerable<ILanguage> additionalLanguages = null,
            string defaultLanguageId = null)
        {
            var languageExtractor = new LanguageExtractor(
                additionalLanguages ?? Enumerable.Empty<ILanguage>(),
                defaultLanguageId ?? string.Empty
            );

            var codeExtractor = new CodeExtractor();
            var htmlFormatter = new HtmlFormatter(StyleDictionary.DefaultDark);
            var colorCodeExtension = new ColorCodeExtension(languageExtractor, codeExtractor, htmlFormatter);

            markdownPipelineBuilder.Extensions.Add(colorCodeExtension);

            return markdownPipelineBuilder;
        }
    }
}
