import MarkdownIt from "markdown-it";
import sanitizeHtml from "sanitize-html";
import { z } from "zod";

const textField = z.string().trim();
const optionalTextField = z.string().trim().default("");

export const articleSchema = z.object({
  id: textField.min(1),
  slug: textField.min(1),
  title: textField.min(1),
  summary: optionalTextField,
  markdown: optionalTextField,
  showOnHome: z.boolean().default(false),
  createdAt: textField.min(1),
  updatedAt: textField.min(1),
});

export const articleCollectionSchema = z.array(articleSchema);

export type Article = z.infer<typeof articleSchema>;
export type ArticlePreviewModel = {
  title: string;
  summary: string;
  bodyHtml: string;
  isEmpty: boolean;
};

const markdown = new MarkdownIt({
  html: false,
  linkify: true,
  breaks: true,
});

function applyImageSizeAnnotations(rendered: string) {
  return rendered.replace(
    /<p>\s*(<img\b[^>]*>)\s*\{size=(small|medium|large|full)\}\s*<\/p>/gi,
    (_match, imageHtml, size) => `<figure class="article-image article-image--${String(size).toLowerCase()}">${imageHtml}</figure>`,
  );
}

export function validateArticles(input: unknown) {
  return articleCollectionSchema.parse(input);
}

export function renderArticleMarkdown(markdownSource: string) {
  const rendered = applyImageSizeAnnotations(markdown.render(String(markdownSource ?? "")));
  return sanitizeHtml(rendered, {
    allowedTags: [
      "h1",
      "h2",
      "h3",
      "h4",
      "p",
      "blockquote",
      "ul",
      "ol",
      "li",
      "strong",
      "em",
      "a",
      "img",
      "hr",
      "code",
      "pre",
      "br",
      "figure",
    ],
    allowedAttributes: {
      a: ["href", "target", "rel"],
      img: ["src", "alt", "title", "loading"],
      figure: ["class"],
      code: ["class"],
    },
    allowedSchemes: ["http", "https", "mailto"],
    allowedSchemesByTag: {
      img: ["http", "https"],
    },
    transformTags: {
      a: sanitizeHtml.simpleTransform("a", {
        target: "_blank",
        rel: "noreferrer",
      }),
      img: (tagName, attribs) => ({
        tagName,
        attribs: {
          ...attribs,
          loading: "lazy",
        },
      }),
    },
  });
}

export function buildArticlePreviewModel(input: { title?: string; summary?: string; markdown?: string }): ArticlePreviewModel {
  const title = String(input.title ?? "").trim();
  const summary = String(input.summary ?? "").trim();
  const bodyHtml = renderArticleMarkdown(String(input.markdown ?? ""));
  return {
    title,
    summary,
    bodyHtml,
    isEmpty: !title && !summary && !bodyHtml.trim(),
  };
}
