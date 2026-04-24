import type { Article } from "./articles.js";
import type { SiteConfig } from "./library-store.js";
import type { LibraryData } from "../../shared/src/schema.js";
import { getWebsiteDisplay } from "../../shared/src/display.js";

export type RecentWorkUpdate = {
  workId: string;
  workTitle: string;
  composerName: string;
  href: string;
  updatedAt: string;
};

export type FeaturedArticleEntry = {
  id: string;
  slug: string;
  title: string;
  summary: string;
  href: string;
  updatedAt: string;
};

export type SiteConfigPatch = Partial<Omit<SiteConfig, "contact">> & {
  contact?: Partial<SiteConfig["contact"]>;
};

export function buildRecentWorkUpdates(library: LibraryData, limit = 5): RecentWorkUpdate[] {
  const composerById = new Map(library.composers.map((composer) => [composer.id, composer]));
  const workById = new Map(library.works.map((work) => [work.id, work]));
  const seenWorkIds = new Set<string>();

  return [...library.recordings]
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .filter((recording) => {
      if (seenWorkIds.has(recording.workId)) {
        return false;
      }
      seenWorkIds.add(recording.workId);
      return true;
    })
    .slice(0, limit)
    .map((recording) => {
      const work = workById.get(recording.workId);
      const composer = work ? composerById.get(work.composerId) : undefined;

      return {
        workId: recording.workId,
        workTitle: work?.title ?? "未知作品",
        composerName: composer ? getWebsiteDisplay(composer).heading : "未知作曲家",
        href: `/works/${recording.workId}/`,
        updatedAt: recording.updatedAt.slice(0, 10),
      };
    });
}

export function buildFeaturedArticles(articles: Article[], limit = 10): FeaturedArticleEntry[] {
  return [...articles]
    .filter((article) => article.showOnHome)
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .slice(0, limit)
    .map((article) => ({
      id: article.id,
      slug: article.slug,
      title: article.title,
      summary: article.summary,
      href: `/columns/${article.slug}/`,
      updatedAt: article.updatedAt.slice(0, 10),
    }));
}

export function mergeSiteConfigPatch(current: SiteConfig, patch: SiteConfigPatch): SiteConfig {
  return {
    ...current,
    ...patch,
    contact: {
      ...current.contact,
      ...(patch.contact ?? {}),
    },
  };
}

