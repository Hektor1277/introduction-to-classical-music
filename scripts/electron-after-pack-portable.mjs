import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as ResEdit from "resedit";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

export default async function afterPack(context) {
  await patchWindowsExecutableIcon(context);
}

async function patchWindowsExecutableIcon(context) {
  if (context.electronPlatformName !== "win32") {
    return;
  }

  const executablePath = path.join(context.appOutDir, `${context.packager.appInfo.productFilename}.exe`);
  const iconPath = path.join(repoRoot, "apps", "desktop", "assets", "app-icon.ico");
  const executableData = await readFile(executablePath);
  const iconData = await readFile(iconPath);
  const exe = ResEdit.NtExecutable.from(executableData);
  const resources = ResEdit.NtExecutableResource.from(exe);
  const iconFile = ResEdit.Data.IconFile.from(iconData);
  const iconGroupEntries = ResEdit.Resource.IconGroupEntry.fromEntries(resources.entries);
  const iconGroupTargets =
    iconGroupEntries.length === 0 ? [{ id: 101, lang: 1033 }] : iconGroupEntries.map((entry) => ({ id: entry.id, lang: entry.lang }));

  for (const target of iconGroupTargets) {
    ResEdit.Resource.IconGroupEntry.replaceIconsForResource(
      resources.entries,
      target.id,
      target.lang,
      iconFile.icons.map((item) => item.data),
    );
  }

  resources.outputResource(exe);
  await writeFile(executablePath, Buffer.from(exe.generate()));
}
