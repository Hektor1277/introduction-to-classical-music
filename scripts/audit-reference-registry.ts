import { promises as fs } from "node:fs";

import {
  auditReferenceRegistry,
  buildReferenceRegistry,
  getOrchestraReferenceDefaultPath,
  getPersonReferenceDefaultPath,
} from "../packages/data-core/src/reference-registry.js";

async function main() {
  const orchestraPath = getOrchestraReferenceDefaultPath();
  const personPath = getPersonReferenceDefaultPath();
  const [orchestraSourceText, personSourceText] = await Promise.all([
    fs.readFile(orchestraPath, "utf8").catch(() => ""),
    fs.readFile(personPath, "utf8").catch(() => ""),
  ]);
  const registry = buildReferenceRegistry({ orchestraSourceText, personSourceText });
  const issues = auditReferenceRegistry(registry);
  console.log(
    JSON.stringify(
      {
        orchestraEntries: registry.orchestraEntries.length,
        personEntries: registry.personEntries.length,
        issueCount: issues.length,
        issues,
      },
      null,
      2,
    ),
  );
}

await main();
