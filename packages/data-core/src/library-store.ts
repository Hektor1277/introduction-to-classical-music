import { promises as fs } from "node:fs";
import path from "node:path";

import { validateArticles, type Article } from "./articles.js";
import { buildIndexes, type PersonLinkConfig } from "./indexes.js";
import { validateLibrary, type InfoPanel, type LibraryData, type ResourceLink } from "../../shared/src/schema.js";
import { getRuntimePaths } from "./app-paths.js";
import { sanitizeResourceLinksForSiteOutput } from "./resource-links.js";

const siteConfigDefaults: {
  title: string;
  subtitle: string;
  description: string;
  heroIntro: string;
  composerDirectoryIntro: string;
  conductorDirectoryIntro: string;
  searchIntro: string;
  about: string[];
  contact: {
    label: string;
    value: string;
  };
  copyrightNotice: string;
  lastImportedAt: string;
} = {
  title: "古典导聆不全书",
  subtitle: "公益性的古典音乐版本导聆目录",
  description: "",
  heroIntro: "",
  composerDirectoryIntro: "先进入作曲家，再从作品类型层级进入具体曲目。默认按姓氏字母排序，也可切换为按出生年份或按国家浏览。",
  conductorDirectoryIntro: "从指挥切入时，版本仍按作曲家与作品归档。目录支持按姓氏字母、出生年份和国家切换，并保留快速定位栏。",
  searchIntro: "未输入时每类保留前 5 条示例；输入并搜索后将按类型展示完整命中结果，并为每类结果提供分页。",
  about: [],
  contact: {
    label: "",
    value: "",
  },
  copyrightNotice: "",
  lastImportedAt: "",
};

export type SiteConfig = typeof siteConfigDefaults;
export type ReviewQueueEntry = {
  entityId: string;
  entityType: "work" | "recording" | "person" | "composer";
  issue: string;
  sourcePath?: string;
  note?: string;
};

function getFileMap() {
  const runtimePaths = getRuntimePaths();
  return {
    composers: path.join(runtimePaths.library.contentLibraryDir, "composers.json"),
    people: path.join(runtimePaths.library.contentLibraryDir, "people.json"),
    personLinks: path.join(runtimePaths.library.contentLibraryDir, "person-links.json"),
    workGroups: path.join(runtimePaths.library.contentLibraryDir, "work-groups.json"),
    works: path.join(runtimePaths.library.contentLibraryDir, "works.json"),
    recordings: path.join(runtimePaths.library.contentLibraryDir, "recordings.json"),
    reviewQueue: path.join(runtimePaths.library.contentLibraryDir, "review-queue.json"),
    site: path.join(runtimePaths.library.contentSiteDir, "config.json"),
    articles: path.join(runtimePaths.library.contentSiteDir, "articles.json"),
    generatedLibrary: path.join(runtimePaths.library.runtimeGeneratedDir, "library.json"),
    generatedIndexes: path.join(runtimePaths.library.runtimeGeneratedDir, "indexes.json"),
    generatedSite: path.join(runtimePaths.library.runtimeGeneratedDir, "site.json"),
    generatedArticles: path.join(runtimePaths.library.runtimeGeneratedDir, "articles.json"),
  } as const;
}

async function ensureDirectories() {
  const runtimePaths = getRuntimePaths();
  await fs.mkdir(runtimePaths.library.contentLibraryDir, { recursive: true });
  await fs.mkdir(runtimePaths.library.contentSiteDir, { recursive: true });
  await fs.mkdir(runtimePaths.library.runtimeGeneratedDir, { recursive: true });
}

async function readJsonFile<T>(filePath: string, fallback: T): Promise<T> {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return fallback;
    }
    throw error;
  }
}

async function writeJsonFile(filePath: string, value: unknown) {
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export async function loadLibraryFromDisk(): Promise<LibraryData> {
  await ensureDirectories();
  const fileMap = getFileMap();

  return validateLibrary({
    composers: await readJsonFile(fileMap.composers, []),
    people: await readJsonFile(fileMap.people, []),
    workGroups: await readJsonFile(fileMap.workGroups, []),
    works: await readJsonFile(fileMap.works, []),
    recordings: await readJsonFile(fileMap.recordings, []),
  });
}

export async function saveLibraryToDisk(library: LibraryData) {
  await ensureDirectories();
  const fileMap = getFileMap();
  const projectedPersonIds = new Set((library.people || []).map((person) => person.id));
  await writeJsonFile(
    fileMap.composers,
    (library.composers || []).filter((composer) => !projectedPersonIds.has(composer.id)),
  );
  await writeJsonFile(fileMap.people, library.people);
  await writeJsonFile(fileMap.workGroups, library.workGroups);
  await writeJsonFile(fileMap.works, library.works);
  await writeJsonFile(fileMap.recordings, library.recordings);
}

export async function loadPersonLinks(): Promise<PersonLinkConfig> {
  await ensureDirectories();
  const fileMap = getFileMap();
  const raw = await readJsonFile<Partial<PersonLinkConfig>>(fileMap.personLinks, {});
  return {
    canonicalPersonLinks: raw.canonicalPersonLinks ?? {},
  };
}

export async function savePersonLinks(config: PersonLinkConfig) {
  await ensureDirectories();
  const fileMap = getFileMap();
  await writeJsonFile(fileMap.personLinks, config);
}

export async function loadReviewQueue() {
  await ensureDirectories();
  const fileMap = getFileMap();
  return readJsonFile<ReviewQueueEntry[]>(fileMap.reviewQueue, []);
}

export async function saveReviewQueue(reviewQueue: ReviewQueueEntry[]) {
  await ensureDirectories();
  const fileMap = getFileMap();
  await writeJsonFile(fileMap.reviewQueue, reviewQueue);
}

export async function loadSiteConfig(): Promise<SiteConfig> {
  await ensureDirectories();
  const fileMap = getFileMap();
  const raw = await readJsonFile<SiteConfig>(fileMap.site, siteConfigDefaults);
  return {
    ...siteConfigDefaults,
    ...raw,
    contact: {
      ...siteConfigDefaults.contact,
      ...raw.contact,
    },
  };
}

export async function loadArticlesFromDisk(): Promise<Article[]> {
  await ensureDirectories();
  const fileMap = getFileMap();
  return validateArticles(await readJsonFile(fileMap.articles, []));
}

export async function saveArticlesToDisk(articles: Article[]) {
  await ensureDirectories();
  const fileMap = getFileMap();
  await writeJsonFile(fileMap.articles, validateArticles(articles));
}

export async function saveSiteConfig(siteConfig: SiteConfig) {
  await ensureDirectories();
  const fileMap = getFileMap();
  await writeJsonFile(fileMap.site, siteConfig);
}

function prepareLibraryForSiteOutput(library: LibraryData, options: { includeLocalOnlyLinks?: boolean } = {}) {
  const sanitizeLinks = (links: ResourceLink[] = []) =>
    sanitizeResourceLinksForSiteOutput(links, { includeLocalOnly: options.includeLocalOnlyLinks });
  const sanitizeInfoPanel = (infoPanel: InfoPanel | undefined) =>
    infoPanel
      ? {
          ...infoPanel,
          collectionLinks: sanitizeLinks(infoPanel.collectionLinks || []),
        }
      : infoPanel;

  return validateLibrary({
    composers: library.composers.map((composer) => ({
      ...composer,
      infoPanel: sanitizeInfoPanel(composer.infoPanel),
    })),
    people: library.people.map((person) => ({
      ...person,
      infoPanel: sanitizeInfoPanel(person.infoPanel),
    })),
    workGroups: library.workGroups,
    works: library.works.map((work) => ({
      ...work,
      infoPanel: sanitizeInfoPanel(work.infoPanel),
    })),
    recordings: library.recordings.map((recording) => ({
      ...recording,
      links: sanitizeLinks(recording.links || []),
      infoPanel: sanitizeInfoPanel(recording.infoPanel),
    })),
  });
}

export async function writeGeneratedArtifacts(options: { includeLocalOnlyLinks?: boolean } = {}) {
  const [library, site, personLinks, articles] = await Promise.all([
    loadLibraryFromDisk(),
    loadSiteConfig(),
    loadPersonLinks(),
    loadArticlesFromDisk(),
  ]);
  const renderableLibrary = prepareLibraryForSiteOutput(library, options);
  const indexes = buildIndexes(renderableLibrary, personLinks, articles);
  const fileMap = getFileMap();

  await ensureDirectories();
  await writeJsonFile(fileMap.generatedLibrary, renderableLibrary);
  await writeJsonFile(fileMap.generatedIndexes, indexes);
  await writeJsonFile(fileMap.generatedSite, site);
  await writeJsonFile(fileMap.generatedArticles, articles);

  return {
    library: renderableLibrary,
    site,
    indexes,
    articles,
  };
}

export async function readGeneratedLibrary() {
  const fileMap = getFileMap();
  return readJsonFile<LibraryData>(
    fileMap.generatedLibrary,
    validateLibrary({
      composers: [],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    }),
  );
}

export async function readGeneratedIndexes() {
  const fileMap = getFileMap();
  return readJsonFile(
    fileMap.generatedIndexes,
    buildIndexes(
      validateLibrary({
        composers: [],
        people: [],
        workGroups: [],
        works: [],
        recordings: [],
      }),
      { canonicalPersonLinks: {} },
      [],
    ),
  );
}

export async function readGeneratedSite() {
  const fileMap = getFileMap();
  return readJsonFile<SiteConfig>(fileMap.generatedSite, siteConfigDefaults);
}

export async function readGeneratedArticles() {
  const fileMap = getFileMap();
  return validateArticles(await readJsonFile(fileMap.generatedArticles, []));
}

export function getDataPaths() {
  return getFileMap();
}
