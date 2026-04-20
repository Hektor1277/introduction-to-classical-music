import fs from "node:fs";
import path from "node:path";

const rootDir = process.cwd();
const reviewPath = path.join(rootDir, "data/library/entity-vitals-review.json");
const composersPath = path.join(rootDir, "data/library/composers.json");
const peoplePath = path.join(rootDir, "data/library/people.json");

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function compact(value) {
  return String(value ?? "").trim();
}

function dedupe(values) {
  return [...new Set(values.map((value) => compact(value)).filter(Boolean))];
}

function normalizeCountryFields(entity) {
  const countries = dedupe([...(Array.isArray(entity.countries) ? entity.countries : []), entity.country]);
  if (countries.length) {
    entity.countries = countries;
    entity.country = countries[0];
    return;
  }

  delete entity.countries;
  entity.country = "";
}

function applyEntry(collection, entry) {
  const index = collection.findIndex((item) => item.id === entry.entityId);
  if (index === -1) {
    throw new Error(`Missing entity for review entry: ${entry.entityId}`);
  }

  const next = {
    ...collection[index],
    ...(entry.set || {}),
  };

  for (const field of entry.removeFields || []) {
    delete next[field];
  }

  normalizeCountryFields(next);

  collection[index] = next;
}

const reviewEntries = readJson(reviewPath);
const composers = readJson(composersPath);
const people = readJson(peoplePath);

for (const entry of reviewEntries) {
  if (entry.entityType === "composer") {
    applyEntry(composers, entry);
    continue;
  }

  if (entry.entityType === "person") {
    applyEntry(people, entry);
    continue;
  }

  throw new Error(`Unsupported entityType in review entry: ${entry.entityType}`);
}

writeJson(composersPath, composers);
writeJson(peoplePath, people);

console.log(
  JSON.stringify(
    {
      appliedEntries: reviewEntries.length,
      reviewPath,
      updatedFiles: [composersPath, peoplePath],
    },
    null,
    2,
  ),
);
