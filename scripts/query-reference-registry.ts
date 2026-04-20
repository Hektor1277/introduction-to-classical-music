import { promises as fs } from "node:fs";

import {
  buildReferenceRegistry,
  getOrchestraReferenceDefaultPath,
  getPersonReferenceDefaultPath,
  lookupOrchestraReference,
  lookupOrchestraReferences,
  lookupPersonReference,
} from "../packages/data-core/src/reference-registry.js";

function compact(value: unknown) {
  return String(value ?? "").trim();
}

async function main() {
  const args = process.argv.slice(2);
  const roleIndex = args.findIndex((arg) => arg === "--role");
  const role = roleIndex >= 0 ? compact(args[roleIndex + 1]) : "";
  const query = compact(args.filter((_, index) => index !== roleIndex && index !== roleIndex + 1).join(" "));
  if (!query) {
    console.error('Usage: node output/runtime/scripts/query-reference-registry.js "<value>" [--role conductor]');
    process.exitCode = 1;
    return;
  }

  const [orchestraSourceText, personSourceText] = await Promise.all([
    fs.readFile(getOrchestraReferenceDefaultPath(), "utf8").catch(() => ""),
    fs.readFile(getPersonReferenceDefaultPath(), "utf8").catch(() => ""),
  ]);
  const registry = buildReferenceRegistry({ orchestraSourceText, personSourceText });

  console.log(
    JSON.stringify(
      {
        query,
        role: role || undefined,
        orchestra: {
          resolved: lookupOrchestraReference(registry, query),
          candidates: lookupOrchestraReferences(registry, query),
        },
        person: lookupPersonReference(registry, query, role || undefined),
      },
      null,
      2,
    ),
  );
}

await main();
