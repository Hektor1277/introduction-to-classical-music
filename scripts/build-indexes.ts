import { writeGeneratedArtifacts } from "../packages/data-core/src/library-store.js";

async function main() {
  const { indexes } = await writeGeneratedArtifacts();

  process.stdout.write(
    `Built indexes for ${indexes.stats.composerCount} composers, ${indexes.stats.workCount} works, ${indexes.stats.recordingCount} recordings.\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});

