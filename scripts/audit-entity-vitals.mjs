import { promises as fs } from "node:fs";
import path from "node:path";

import { auditEntityVitals } from "../output/runtime/packages/data-core/src/entity-vitals-audit.js";
import { loadLibraryFromDisk } from "../output/runtime/packages/data-core/src/library-store.js";

async function main() {
  const library = await loadLibraryFromDisk();
  const result = auditEntityVitals(library);
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputDir = path.join(process.cwd(), "output", "entity-vitals-audit");
  await fs.mkdir(outputDir, { recursive: true });
  const jsonPath = path.join(outputDir, `entity-vitals-audit-${timestamp}.json`);
  await fs.writeFile(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  console.log(
    JSON.stringify(
      {
        ...result,
        artifacts: {
          jsonPath,
        },
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: error instanceof Error ? error.stack || error.message : String(error),
      },
      null,
      2,
    ),
  );
  process.exitCode = 1;
});
