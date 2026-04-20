import express from "express";
import type { Server } from "node:http";
import { once } from "node:events";
import { promises as fs } from "node:fs";
import net from "node:net";
import path from "node:path";

import { openTargetInShell } from "./open-target.js";

async function findAvailablePort(preferredPort: number) {
  let port = preferredPort;
  while (true) {
    const available = await new Promise<boolean>((resolve) => {
      const tester = net.createServer();
      tester.once("error", () => resolve(false));
      tester.once("listening", () => {
        tester.close(() => resolve(true));
      });
      tester.listen(port, "127.0.0.1");
    });
    if (available) {
      return port;
    }
    port += 1;
  }
}

export function createLocalSiteServer(options: {
  host?: string;
  preferredPort?: number;
  onOpenLocalPath?: (targetPath: string) => Promise<void> | void;
} = {}) {
  const host = options.host || "127.0.0.1";
  const preferredPort = options.preferredPort || 4331;
  const onOpenLocalPath = options.onOpenLocalPath || openTargetInShell;
  let server: Server | null = null;
  let url = "";
  let rootDir = "";

  return {
    async ensureStarted(nextRootDir: string) {
      const resolvedRootDir = path.resolve(nextRootDir);
      await fs.access(path.join(resolvedRootDir, "index.html"));
      if (server && rootDir === resolvedRootDir) {
        return { url, rootDir };
      }

      if (server) {
        await new Promise<void>((resolve, reject) => {
          server?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        server = null;
      }

      const port = await findAvailablePort(preferredPort);
      const app = express();
      app.get("/__local-resource", async (request, response) => {
        const targetPath = String(request.query.path || "").trim();
        if (!targetPath) {
          response.status(400).send("Missing local resource path.");
          return;
        }
        try {
          await fs.access(targetPath);
          await onOpenLocalPath(targetPath);
          response
            .status(200)
            .type("html")
            .send("<!doctype html><meta charset=\"utf-8\"><title>Opened</title><p>Local resource opened.</p>");
        } catch (error) {
          response
            .status(404)
            .type("html")
            .send(`<!doctype html><meta charset="utf-8"><title>Unavailable</title><p>${error instanceof Error ? error.message : String(error)}</p>`);
        }
      });
      app.use(express.static(resolvedRootDir));
      app.get(/.*/, (_request, response) => {
        response.sendFile(path.join(resolvedRootDir, "index.html"));
      });

      server = app.listen(port, host);
      await once(server, "listening");
      rootDir = resolvedRootDir;
      url = `http://${host}:${port}`;
      return { url, rootDir };
    },
    async stop() {
      if (!server) {
        return;
      }
      await new Promise<void>((resolve, reject) => {
        server?.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
      server = null;
      url = "";
      rootDir = "";
    },
    getState() {
      return {
        running: Boolean(server),
        url,
        rootDir,
      };
    },
  };
}
