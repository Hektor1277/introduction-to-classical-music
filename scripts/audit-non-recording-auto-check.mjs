import { promises as fs } from "node:fs";
import path from "node:path";

import { loadLibraryFromDisk } from "../output/runtime/packages/data-core/src/library-store.js";
import { createAutomationJobManager } from "../output/runtime/packages/automation/src/automation-jobs.js";
import { loadLlmConfig } from "../output/runtime/packages/automation/src/automation-store.js";
import { fetchWithWindowsFallback } from "../output/runtime/packages/automation/src/external-fetch.js";

const outputDir = path.join(process.cwd(), "output", "audits");
const categories = ["composer", "conductor", "orchestra", "artist", "work"];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowStamp() {
  return new Date().toISOString().replaceAll(":", "-");
}

function summarizeJob(job) {
  const byCategory = Object.fromEntries(
    categories.map((category) => {
      const items = job.items.filter((item) => item.category === category);
      const issueCounts = new Map();
      for (const item of items) {
        for (const issue of item.reviewIssues || []) {
          issueCounts.set(issue, (issueCounts.get(issue) || 0) + 1);
        }
      }
      const topIssues = [...issueCounts.entries()]
        .sort((left, right) => right[1] - left[1])
        .slice(0, 8)
        .map(([issue, count]) => ({ issue, count }));
      const samples = items
        .filter((item) => item.status === "needs-attention" || item.status === "failed")
        .slice(0, 10)
        .map((item) => ({
          entityId: item.entityId,
          label: item.label,
          status: item.status,
          reviewIssues: item.reviewIssues || [],
          errors: item.errors || [],
        }));
      return [
        category,
        {
          total: items.length,
          succeeded: items.filter((item) => item.status === "succeeded").length,
          needsAttention: items.filter((item) => item.status === "needs-attention").length,
          completedNoChange: items.filter((item) => item.status === "completed-nochange").length,
          failed: items.filter((item) => item.status === "failed").length,
          topIssues,
          samples,
        },
      ];
    }),
  );

  const proposalBreakdown = categories.map((category) => {
    const proposals = (job.run?.proposals || []).filter((proposal) => proposal.entityType === (category === "composer" ? "composer" : category === "work" ? "work" : "person"));
    return {
      category,
      total: proposals.length,
      pending: proposals.filter((proposal) => proposal.status === "pending").length,
      lowRisk: proposals.filter((proposal) => proposal.risk === "low").length,
      mediumRisk: proposals.filter((proposal) => proposal.risk === "medium").length,
      highRisk: proposals.filter((proposal) => proposal.risk === "high").length,
    };
  });

  return {
    jobId: job.id,
    status: job.status,
    progress: job.progress,
    llmEnabled: true,
    byCategory,
    proposalBreakdown,
    errorCount: job.errors.length,
    errors: job.errors,
  };
}

function toMarkdown(summary) {
  const lines = [];
  lines.push("# Non-Recording Auto Check Audit");
  lines.push("");
  lines.push(`- Job: \`${summary.jobId}\``);
  lines.push(`- Status: \`${summary.status}\``);
  lines.push(
    `- Progress: total ${summary.progress.total}, processed ${summary.progress.processed}, succeeded ${summary.progress.succeeded}, unchanged ${summary.progress.unchanged}, attention ${summary.progress.attention}, failed ${summary.progress.failed}`,
  );
  lines.push("");
  lines.push("## Category Summary");
  lines.push("");
  for (const category of categories) {
    const stats = summary.byCategory[category];
    lines.push(`### ${category}`);
    lines.push("");
    lines.push(`- Total: ${stats.total}`);
    lines.push(`- Succeeded: ${stats.succeeded}`);
    lines.push(`- Needs attention: ${stats.needsAttention}`);
    lines.push(`- Completed no change: ${stats.completedNoChange}`);
    lines.push(`- Failed: ${stats.failed}`);
    if (stats.topIssues.length) {
      lines.push("- Top issues:");
      for (const issue of stats.topIssues) {
        lines.push(`  - ${issue.count} x ${issue.issue}`);
      }
    }
    if (stats.samples.length) {
      lines.push("- Sample problematic items:");
      for (const sample of stats.samples) {
        const issueText = [...sample.reviewIssues, ...sample.errors].filter(Boolean).join(" | ");
        lines.push(`  - ${sample.label} (\`${sample.entityId}\`, ${sample.status}): ${issueText}`);
      }
    }
    lines.push("");
  }
  lines.push("## Proposal Breakdown");
  lines.push("");
  for (const item of summary.proposalBreakdown) {
    lines.push(
      `- ${item.category}: total ${item.total}, pending ${item.pending}, low ${item.lowRisk}, medium ${item.mediumRisk}, high ${item.highRisk}`,
    );
  }
  lines.push("");
  if (summary.errors.length) {
    lines.push("## Job Errors");
    lines.push("");
    for (const error of summary.errors) {
      lines.push(`- [${error.entityType || "job"}:${error.entityId || "-"}] ${error.code}: ${error.message}`);
    }
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(outputDir, { recursive: true });
  const library = await loadLibraryFromDisk();
  const llmConfig = await loadLlmConfig();
  const manager = createAutomationJobManager();
  const fetchImpl = (input, init) => fetchWithWindowsFallback(input, init, { fetchImpl: fetch });
  const request = { categories };

  console.log(`[audit] starting categories=${categories.join(",")} llm=${llmConfig.enabled ? "enabled" : "disabled"}`);
  const job = manager.createJob({
    library,
    request,
    fetchImpl,
    llmConfig,
    maxConcurrency: 8,
  });

  let lastProgress = "";
  while (true) {
    const current = manager.getJob(job.id);
    if (!current) {
      throw new Error(`Job disappeared: ${job.id}`);
    }
    const progressText = `${current.status}:${current.progress.processed}/${current.progress.total} s=${current.progress.succeeded} u=${current.progress.unchanged} a=${current.progress.attention} f=${current.progress.failed}`;
    if (progressText !== lastProgress) {
      console.log(`[audit] ${progressText}`);
      lastProgress = progressText;
    }
    if (current.status === "completed" || current.status === "cancelled") {
      const summary = summarizeJob(current);
      const stamp = nowStamp();
      const jsonPath = path.join(outputDir, `non-recording-auto-check-${stamp}.json`);
      const mdPath = path.join(outputDir, `non-recording-auto-check-${stamp}.md`);
      await fs.writeFile(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
      await fs.writeFile(mdPath, toMarkdown(summary), "utf8");
      console.log(`[audit] wrote ${jsonPath}`);
      console.log(`[audit] wrote ${mdPath}`);
      return;
    }
    await sleep(2000);
  }
}

await main();
