import { spawn } from "node:child_process";

function getOpenCommand() {
  if (process.platform === "win32") {
    return { command: "explorer.exe", args: (target: string) => [target] };
  }
  if (process.platform === "darwin") {
    return { command: "open", args: (target: string) => [target] };
  }
  return { command: "xdg-open", args: (target: string) => [target] };
}

export async function openTargetInShell(target: string) {
  const normalizedTarget = String(target ?? "").trim();
  if (!normalizedTarget) {
    throw new Error("Open target cannot be empty");
  }

  const opener = getOpenCommand();

  await new Promise<void>((resolve, reject) => {
    const child = spawn(opener.command, opener.args(normalizedTarget), {
      detached: true,
      stdio: "ignore",
      windowsHide: true,
    });
    child.once("error", reject);
    child.once("spawn", () => {
      child.unref();
      resolve();
    });
  });
}
