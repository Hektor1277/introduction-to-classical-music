// @ts-nocheck
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { load } from "cheerio";

import { parseLegacyRecordingHtml } from "../packages/data-core/src/legacy-parser.js";
import { auditResourceLinks } from "../packages/data-core/src/resource-links.js";
import {
  loadSiteConfig,
  saveLibraryToDisk,
  saveReviewQueue,
  saveSiteConfig,
  writeGeneratedArtifacts,
  type ReviewQueueEntry,
} from "../packages/data-core/src/library-store.js";
import { validateLibrary, type Composer, type Credit, type LibraryData, type Person, type Recording, type Work, type WorkGroup } from "../packages/shared/src/schema.js";
import { createEntityId, createSlug, createSortKey, ensureUniqueValue } from "../packages/shared/src/slug.js";

type ImportContext = {
  composers: Composer[];
  people: Person[];
  workGroups: WorkGroup[];
  works: Work[];
  recordings: Recording[];
  reviewQueue: ReviewQueueEntry[];
  personByName: Map<string, Person>;
  composerByName: Map<string, Composer>;
  groupByPath: Map<string, WorkGroup>;
};

const SOURCE_ROOT_NAME = "an incomplete guide to classical music";
const defaultSources = [
  path.join(process.cwd(), "materials", "archive", "an incomplete guide to classical music.rar"),
  path.join(process.cwd(), "materials", "archive", "古典导聆不全书 2025.09.29.chm"),
];

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function normalizeWhitespace(value: string) {
  return value.replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();
}

function normalizeNameKey(value: string) {
  return value
    .normalize("NFKC")
    .replace(/[\s"'`.,·，。；：、_\-]+/g, "")
    .toLowerCase();
}

function toPublicAssetPath(value: string) {
  const normalized = value.replace(/\\/g, "/").replace(/^(\.\.\/)+/, "");
  return `/library-assets/legacy/${normalized}`;
}

function titleCandidateFromFileName(value: string) {
  return normalizeWhitespace(
    value
      .replace(/\.htm$/i, "")
      .replace(/_&_+/g, " & ")
      .replace(/[_]+/g, " ")
      .replace(/([^\d])(\d{4})$/, "$1 $2"),
  );
}

function removeTrailingYear(value: string) {
  return value
    .replace(/[_\-. ,]*(\d{4}(?:[._-]\d{1,4})?)$/u, "")
    .replace(/[_\-. ,]+$/u, "")
    .trim();
}

function detectLeadRole(groupPath: string[], workTitle: string) {
  const joined = `${groupPath.join(" / ")} ${workTitle}`;
  if (/(交响|交响诗|序曲|歌剧|弥撒|安魂曲|oratorio|symphony|opera|mass|requiem|overture)/i.test(joined)) {
    return "conductor" as const;
  }
  return "soloist" as const;
}

async function exists(targetPath: string) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function extractArchive(sourcePath: string) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "classical-guide-"));

  if (sourcePath.toLowerCase().endsWith(".rar")) {
    const result = spawnSync("tar", ["-xf", sourcePath, "-C", tempDir], {
      stdio: "pipe",
      encoding: "utf8",
    });

    if (result.error) {
      throw result.error;
    }
  } else if (sourcePath.toLowerCase().endsWith(".chm")) {
    spawnSync("C:\\Windows\\hh.exe", ["-decompile", tempDir, sourcePath], {
      stdio: "ignore",
      windowsHide: true,
    });

    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      if (await findLegacyRoot(tempDir)) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  } else {
    throw new Error(`Unsupported source format: ${sourcePath}`);
  }

  return tempDir;
}

async function findLegacyRoot(startDir: string): Promise<string | null> {
  const candidates = [startDir];

  while (candidates.length > 0) {
    const currentDir = candidates.shift();
    if (!currentDir) {
      continue;
    }

    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    if (entries.some((entry) => entry.isDirectory() && entry.name === "作曲家")) {
      return currentDir;
    }

    for (const entry of entries) {
      if (entry.isDirectory()) {
        candidates.push(path.join(currentDir, entry.name));
      }
    }
  }

  return null;
}

async function loadHtmlDocument(filePath: string) {
  const html = await fs.readFile(filePath, "utf8");
  return load(html);
}

async function extractParagraphs(filePath: string) {
  const $ = await loadHtmlDocument(filePath);
  return $("p")
    .toArray()
    .map((paragraph) => normalizeWhitespace($(paragraph).text()))
    .filter(Boolean);
}

function extractNamePair(text: string, fallbackName: string) {
  const normalized = normalizeWhitespace(text);
  const match = normalized.match(/^(.+?)[(（]([^()（）]+)[)）]/);

  return {
    name: normalizeWhitespace(match?.[1] ?? fallbackName),
    nameLatin: normalizeWhitespace(match?.[2] ?? ""),
  };
}

async function parseBioPage(filePath: string, fallbackName: string) {
  const paragraphs = await extractParagraphs(filePath);
  const lead = paragraphs[0] ?? fallbackName;
  const { name, nameLatin } = extractNamePair(lead, fallbackName);
  const summary = paragraphs.slice(1).join("\n\n");

  return {
    name,
    nameLatin,
    summary,
  };
}

function upsertPerson(context: ImportContext, input: Partial<Person> & Pick<Person, "name">) {
  const key = normalizeNameKey(input.name);
  const existing = context.personByName.get(key);

  if (existing) {
    const nextRoles = new Set([...existing.roles, ...(input.roles ?? [])]);
    existing.roles = [...nextRoles];
    existing.aliases = [...new Set([...existing.aliases, ...(input.aliases ?? [])])];
    if (!existing.nameLatin && input.nameLatin) {
      existing.nameLatin = input.nameLatin;
    }
    if (!existing.summary && input.summary) {
      existing.summary = input.summary;
    }
    return existing;
  }

  const requestedRoles = new Set(input.roles ?? []);
  const fuzzyCandidates = context.people.filter((person) => {
    const names = [person.name, ...person.aliases].map(normalizeNameKey);
    const roleMatches =
      requestedRoles.size === 0 || person.roles.some((role) => requestedRoles.has(role));

    return roleMatches && names.some((name) => name.includes(key) || key.includes(name));
  });

  if (fuzzyCandidates.length === 1) {
    const fuzzy = fuzzyCandidates[0];
    fuzzy.aliases = [...new Set([...fuzzy.aliases, input.name, ...(input.aliases ?? [])])];
    if (!fuzzy.nameLatin && input.nameLatin) {
      fuzzy.nameLatin = input.nameLatin;
    }
    if (!fuzzy.summary && input.summary) {
      fuzzy.summary = input.summary;
    }
    const nextRoles = new Set([...fuzzy.roles, ...(input.roles ?? [])]);
    fuzzy.roles = [...nextRoles];
    context.personByName.set(key, fuzzy);
    return fuzzy;
  }

  const existingIds = new Set(context.people.map((person) => person.id));
  const baseId = createEntityId("person", input.slug ?? input.name);
  const id = ensureUniqueValue(baseId, existingIds);

  const person: Person = {
    id,
    slug: input.slug ?? createSlug(input.name),
    name: input.name,
    nameLatin: input.nameLatin ?? "",
    roles: input.roles ?? ["other"],
    aliases: input.aliases ?? [],
    sortKey: input.sortKey ?? createSortKey(context.people.length),
    summary: input.summary ?? "",
  };

  context.people.push(person);
  context.personByName.set(key, person);
  return person;
}

function upsertComposer(context: ImportContext, input: Partial<Composer> & Pick<Composer, "name">) {
  const key = normalizeNameKey(input.name);
  const existing = context.composerByName.get(key);

  if (existing) {
    existing.aliases = [...new Set([...existing.aliases, ...(input.aliases ?? [])])];
    if (!existing.nameLatin && input.nameLatin) {
      existing.nameLatin = input.nameLatin;
    }
    if (!existing.summary && input.summary) {
      existing.summary = input.summary;
    }
    return existing;
  }

  const existingIds = new Set(context.composers.map((composer) => composer.id));
  const baseId = createEntityId("composer", input.slug ?? input.name);
  const id = ensureUniqueValue(baseId, existingIds);

  const composer: Composer = {
    id,
    slug: input.slug ?? createSlug(input.name),
    name: input.name,
    nameLatin: input.nameLatin ?? "",
    aliases: input.aliases ?? [],
    sortKey: input.sortKey ?? createSortKey(context.composers.length),
    summary: input.summary ?? "",
  };

  context.composers.push(composer);
  context.composerByName.set(key, composer);
  return composer;
}

function ensureWorkGroup(context: ImportContext, composer: Composer, groupPath: string[]) {
  const groupIds: string[] = [];

  for (let index = 0; index < groupPath.length; index += 1) {
    const partialPath = groupPath.slice(0, index + 1);
    const cacheKey = `${composer.id}:${partialPath.join("/")}`;
    const cached = context.groupByPath.get(cacheKey);

    if (cached) {
      groupIds.push(cached.id);
      continue;
    }

    const existingIds = new Set(context.workGroups.map((group) => group.id));
    const baseId = createEntityId(`group-${composer.slug}`, partialPath.join("-"));
    const id = ensureUniqueValue(baseId, existingIds);
    const group: WorkGroup = {
      id,
      composerId: composer.id,
      title: partialPath.at(-1) ?? "",
      slug: createSlug(partialPath.at(-1) ?? ""),
      path: partialPath,
      sortKey: createSortKey(context.workGroups.length),
    };

    context.workGroups.push(group);
    context.groupByPath.set(cacheKey, group);
    groupIds.push(group.id);
  }

  return groupIds;
}

async function copyLegacyAssets(rootDir: string) {
  const sourceDir = path.join(rootDir, "pic");
  const targetDir = path.join(process.cwd(), "apps", "site", "public", "library-assets", "legacy", "pic");
  if (!(await exists(sourceDir))) {
    return;
  }

  await fs.mkdir(path.dirname(targetDir), { recursive: true });
  await fs.rm(targetDir, { recursive: true, force: true });
  await fs.cp(sourceDir, targetDir, { recursive: true, force: true });
}

async function collectLeafWorkDirectories(composersDir: string) {
  const workDirs: Array<{ dirPath: string; relativeParts: string[]; htmlFiles: string[] }> = [];

  async function visit(dirPath: string, relativeParts: string[]) {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const childDirs = entries.filter((entry) => entry.isDirectory());
    const htmlFiles = entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".htm"))
      .map((entry) => path.join(dirPath, entry.name));

    if (relativeParts.length >= 3 && childDirs.length === 0 && htmlFiles.length > 1) {
      workDirs.push({ dirPath, relativeParts, htmlFiles });
    }

    for (const childDir of childDirs) {
      await visit(path.join(dirPath, childDir.name), [...relativeParts, childDir.name]);
    }
  }

  const composerDirs = await fs.readdir(composersDir, { withFileTypes: true });
  for (const composerDir of composerDirs) {
    if (!composerDir.isDirectory()) {
      continue;
    }
    await visit(path.join(composersDir, composerDir.name), [composerDir.name]);
  }

  return workDirs;
}

async function chooseWorkSummaryFile(workDirName: string, htmlFiles: string[]) {
  const target = normalizeNameKey(workDirName);
  let best: { filePath: string; score: number } | null = null;

  for (const filePath of htmlFiles) {
    const $ = await loadHtmlDocument(filePath);
    const bodyText = normalizeWhitespace($("body").text());
    const pageTitle = normalizeNameKey($("title").text());
    const fileBase = normalizeNameKey(path.basename(filePath, ".htm"));
    const externalLinks = $("a")
      .toArray()
      .filter((element) => /^https?:\/\//.test($(element).attr("href") ?? "")).length;

    let score = 0;
    if (externalLinks === 0) {
      score += 8;
    }
    if (bodyText.includes("璧勬簮閾炬帴")) {
      score -= 8;
    }
    if (fileBase === target) {
      score += 10;
    }
    if (pageTitle === target) {
      score += 6;
    }
    if (bodyText.length > 120) {
      score += 1;
    }

    if (!best || score > best.score) {
      best = { filePath, score };
    }
  }

  return best?.filePath ?? htmlFiles[0];
}

async function parseWorkSummary(filePath: string, fallbackTitle: string) {
  const $ = await loadHtmlDocument(filePath);
  const pageTitle = normalizeWhitespace($("title").first().text()) || fallbackTitle;
  const paragraphs = $("p")
    .toArray()
    .map((paragraph) => normalizeWhitespace($(paragraph).text()))
    .filter(Boolean);

  const latinLine = paragraphs.find((paragraph) => /[A-Za-z]{4}/.test(paragraph)) ?? "";
  const summary = paragraphs
    .filter((paragraph) => paragraph !== pageTitle && paragraph !== latinLine)
    .join("\n\n");

  return {
    title: fallbackTitle,
    titleLatin: latinLine,
    summary,
  };
}

function inferCreditsFromFileName(
  fileName: string,
  parsedCredits: Credit[],
  groupPath: string[],
  workTitle: string,
  context: ImportContext,
) {
  const creditMatchesFragment = (credit: Credit, fragment: string) => {
    const fragmentKey = normalizeNameKey(fragment);
    if (!fragmentKey) {
      return false;
    }

    const linkedPerson = compact(credit.personId)
      ? context.people.find((person) => person.id === credit.personId)
      : null;
    const candidateKeys = [
      credit.displayName,
      linkedPerson?.name,
      linkedPerson?.nameLatin,
      ...(linkedPerson?.aliases ?? []),
    ]
      .map((value) => normalizeNameKey(value))
      .filter(Boolean);

    return candidateKeys.some((candidate) => candidate === fragmentKey || candidate.includes(fragmentKey) || fragmentKey.includes(candidate));
  };

  const normalized = removeTrailingYear(fileName)
    .replace(/_&_+/g, "&")
    .replace(/＆/g, "&");

  if (!normalized) {
    return parsedCredits;
  }

  const fragments = normalized
    .split("&")
    .map((fragment) => normalizeWhitespace(fragment.replace(/_/g, " ")))
    .filter(Boolean);

  if (fragments.length === 0) {
    return parsedCredits;
  }

  const credits = [...parsedCredits];
  const conductorCredit = credits.find((credit) => credit.role === "conductor");
  const leadRole = detectLeadRole(groupPath, workTitle);

  for (const fragment of fragments) {
    const alreadyIncluded = credits.some((credit) => creditMatchesFragment(credit, fragment));

    if (alreadyIncluded) {
      continue;
    }

    let role: Credit["role"] = leadRole;
    if (conductorCredit) {
      role = "soloist";
    }

    const person = upsertPerson(context, {
      name: fragment,
      roles: [role],
      summary: "",
    });

    credits.push({
      role,
      personId: person.id,
      displayName: person.name,
      label: role === "soloist" ? "文件名补全" : "文件名推断",
    });
  }

  return credits;
}

function isPlaceholderEntityName(value: string) {
  const normalized = normalizeWhitespace(value);
  return !normalized || normalized === "-" || normalized === "未知";
}

function hydrateCredits(context: ImportContext, credits: Credit[]) {
  return credits.flatMap((credit) => {
    if (isPlaceholderEntityName(credit.displayName)) {
      return [];
    }
    const person = upsertPerson(context, {
      name: credit.displayName,
      roles: [credit.role],
      summary: "",
    });

    return [{
      ...credit,
      personId: person.id,
      displayName: person.name,
    }];
  });
}

async function importLibrary(rootDir: string) {
  const context: ImportContext = {
    composers: [],
    people: [],
    workGroups: [],
    works: [],
    recordings: [],
    reviewQueue: [],
    personByName: new Map(),
    composerByName: new Map(),
    groupByPath: new Map(),
  };

  const composersDir = path.join(rootDir, "作曲家");
  const conductorsDir = path.join(rootDir, "指挥家");
  const soloistsDir = path.join(rootDir, "独奏家");
  const importedAt = new Date().toISOString();

  const composerDirs = await fs.readdir(composersDir, { withFileTypes: true });
  for (const composerDir of composerDirs) {
    if (!composerDir.isDirectory()) {
      continue;
    }
    const composerRoot = path.join(composersDir, composerDir.name);
    const composerRootEntries = await fs.readdir(composerRoot, { withFileTypes: true });
    const bioFileEntry = composerRootEntries.find(
      (entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".htm"),
    );
    const parsed = bioFileEntry
      ? await parseBioPage(path.join(composerRoot, bioFileEntry.name), composerDir.name)
      : { name: composerDir.name, nameLatin: "", summary: "" };

    upsertComposer(context, {
      name: composerDir.name,
      nameLatin: parsed.nameLatin,
      aliases:
        normalizeNameKey(parsed.name) !== normalizeNameKey(composerDir.name) ? [parsed.name] : [],
      summary: parsed.summary,
    });
  }

  if (await exists(conductorsDir)) {
    const conductorFiles = await fs.readdir(conductorsDir, { withFileTypes: true });
    for (const conductorFile of conductorFiles) {
      if (!conductorFile.isFile() || !conductorFile.name.toLowerCase().endsWith(".htm")) {
        continue;
      }

      const parsed = await parseBioPage(
        path.join(conductorsDir, conductorFile.name),
        path.basename(conductorFile.name, ".htm"),
      );

      upsertPerson(context, {
        name: parsed.name,
        nameLatin: parsed.nameLatin,
        summary: parsed.summary,
        roles: ["conductor"],
      });
    }
  }

  if (await exists(soloistsDir)) {
    const soloistCategories = await fs.readdir(soloistsDir, { withFileTypes: true });
    for (const category of soloistCategories) {
      if (!category.isDirectory()) {
        continue;
      }
      const categoryDir = path.join(soloistsDir, category.name);
      const soloistFiles = await fs.readdir(categoryDir, { withFileTypes: true });
      for (const soloistFile of soloistFiles) {
        if (!soloistFile.isFile() || !soloistFile.name.toLowerCase().endsWith(".htm")) {
          continue;
        }

        const parsed = await parseBioPage(
          path.join(categoryDir, soloistFile.name),
          path.basename(soloistFile.name, ".htm"),
        );

        upsertPerson(context, {
          name: parsed.name,
          nameLatin: parsed.nameLatin,
          summary: parsed.summary,
          roles: ["soloist"],
        });
      }
    }
  }

  const workDirectories = await collectLeafWorkDirectories(composersDir);

  for (const workDirectory of workDirectories) {
    const [composerName, ...nestedParts] = workDirectory.relativeParts;
    const workTitle = nestedParts.at(-1) ?? "";
    const groupPath = nestedParts.slice(0, -1);
    const composer = upsertComposer(context, { name: composerName, summary: "" });
    const groupIds = ensureWorkGroup(context, composer, groupPath);
    const summaryFile = await chooseWorkSummaryFile(workTitle, workDirectory.htmlFiles);
    const summary = await parseWorkSummary(summaryFile, workTitle);

    const existingWorkIds = new Set(context.works.map((work) => work.id));
    const baseWorkId = createEntityId(`work-${composer.slug}`, `${groupPath.join("-")}-${workTitle}`);
    const workId = ensureUniqueValue(baseWorkId, existingWorkIds);
    const work: Work = {
      id: workId,
      composerId: composer.id,
      groupIds,
      slug: createSlug(workTitle),
      title: summary.title,
      titleLatin: summary.titleLatin,
      aliases: [],
      catalogue: "",
      summary: summary.summary,
      sortKey: createSortKey(context.works.length),
      updatedAt: importedAt,
    };
    context.works.push(work);

    const recordingFiles = workDirectory.htmlFiles.filter((filePath) => filePath !== summaryFile);

    for (const [recordingIndex, recordingFile] of recordingFiles.entries()) {
      const relativeLegacyPath = path
        .relative(rootDir, recordingFile)
        .replace(/\\/g, "/");
      const fileName = path.basename(recordingFile, ".htm");
      const html = await fs.readFile(recordingFile, "utf8");
      const parsedRecording = parseLegacyRecordingHtml(html);
      let credits = hydrateCredits(context, parsedRecording.credits);
      credits = inferCreditsFromFileName(fileName, credits, groupPath, workTitle, context);

      const recordingId = ensureUniqueValue(
        createEntityId(`recording-${work.slug}`, fileName),
        new Set(context.recordings.map((recording) => recording.id)),
      );

      const recording: Recording = {
        id: recordingId,
        workId: work.id,
        slug: createSlug(fileName),
        title: titleCandidateFromFileName(fileName),
        sortKey: createSortKey(recordingIndex),
        isPrimaryRecommendation: recordingIndex === 0,
        updatedAt: importedAt,
        images: parsedRecording.images.map((image) => ({
          ...image,
          src: toPublicAssetPath(image.src),
        })),
        credits,
        links: parsedRecording.links,
        notes: "",
        performanceDateText: parsedRecording.performanceDateText,
        venueText: parsedRecording.venueText,
        albumTitle: parsedRecording.albumTitle,
        label: parsedRecording.label,
        releaseDate: parsedRecording.releaseDate,
        legacyPath: relativeLegacyPath,
      };

      context.recordings.push(recording);

      if (!recording.performanceDateText) {
        context.reviewQueue.push({
          entityId: recording.id,
          entityType: "recording",
          issue: "missing-performance-date",
          sourcePath: relativeLegacyPath,
        });
      }

      if (recording.images.length === 0) {
        context.reviewQueue.push({
          entityId: recording.id,
          entityType: "recording",
          issue: "missing-image",
          sourcePath: relativeLegacyPath,
        });
      }

      if (!recording.albumTitle && !recording.label && !recording.releaseDate) {
        context.reviewQueue.push({
          entityId: recording.id,
          entityType: "recording",
          issue: "missing-album-metadata",
          sourcePath: relativeLegacyPath,
        });
      }

      for (const issue of auditResourceLinks(recording.links)) {
        context.reviewQueue.push({
          entityId: recording.id,
          entityType: "recording",
          issue: `resource-link-${issue.code}`,
          sourcePath: relativeLegacyPath,
          note: issue.message,
        });
      }
    }
  }

  return {
    library: validateLibrary({
      composers: context.composers,
      people: context.people,
      workGroups: context.workGroups,
      works: context.works,
      recordings: context.recordings,
    }),
    reviewQueue: context.reviewQueue,
  };
}

async function main() {
  const discoveredSource = process.argv[2] ?? (await (async () => {
    for (const candidate of defaultSources) {
      if (await exists(candidate)) {
        return candidate;
      }
    }
    return null;
  })());
  const sourcePath = discoveredSource ?? undefined;
  if (!sourcePath) {
    throw new Error("No legacy source archive found.");
  }

  const tempDir = await extractArchive(sourcePath);
  const rootDir = await findLegacyRoot(tempDir);
  if (!rootDir) {
    throw new Error(`Unable to locate legacy root after extracting ${sourcePath}`);
  }

  await copyLegacyAssets(rootDir);
  const { library, reviewQueue } = await importLibrary(rootDir);
  await saveLibraryToDisk(library);
  await saveReviewQueue(reviewQueue);

  const siteConfig = await loadSiteConfig();
  siteConfig.lastImportedAt = new Date().toISOString();
  await saveSiteConfig(siteConfig);
  await writeGeneratedArtifacts();
  await fs.rm(tempDir, { recursive: true, force: true });

  process.stdout.write(
    `Imported ${library.composers.length} composers, ${library.works.length} works and ${library.recordings.length} recordings from ${path.basename(sourcePath)}.\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});





