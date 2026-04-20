import process from "node:process";

import { collectTextAuditIssues } from "./lib/text-audit.js";

const rootDir = process.cwd();
const issues = await collectTextAuditIssues(rootDir);

const result = {
  summary: {
    totalIssues: issues.length,
  },
  issues,
};

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);

if (issues.length > 0) {
  process.exitCode = 1;
}
