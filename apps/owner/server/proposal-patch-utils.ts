import type { AutomationEntityType, AutomationFieldPatch, AutomationProposal, AutomationRun } from "../../../packages/automation/src/automation.js";

const compact = (value: unknown) => String(value ?? "").trim();

function parseIntegerPatchValue(value: unknown) {
  if (typeof value === "number" && Number.isInteger(value)) {
    return value;
  }
  const normalized = compact(value);
  if (!normalized) {
    return undefined;
  }
  if (/^-?\d+$/.test(normalized)) {
    return Number(normalized);
  }
  return undefined;
}

function parseStringListPatchValue(value: unknown) {
  if (Array.isArray(value)) {
    return value.map((item) => compact(item)).filter(Boolean);
  }
  return String(value ?? "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function sanitizeProposalPatchMap(entityType: AutomationEntityType, patchMap: Record<string, unknown>) {
  const nextPatchMap: Record<string, unknown> = { ...patchMap };

  if (entityType === "composer" || entityType === "person") {
    if ("birthYear" in nextPatchMap) {
      nextPatchMap.birthYear = parseIntegerPatchValue(nextPatchMap.birthYear);
    }
    if ("deathYear" in nextPatchMap) {
      nextPatchMap.deathYear = parseIntegerPatchValue(nextPatchMap.deathYear);
    }
  }

  if ("aliases" in nextPatchMap) {
    nextPatchMap.aliases = parseStringListPatchValue(nextPatchMap.aliases);
  }

  if (entityType === "person" && "roles" in nextPatchMap) {
    nextPatchMap.roles = parseStringListPatchValue(nextPatchMap.roles);
  }

  return nextPatchMap;
}

export function sanitizeProposalFields(
  entityType: AutomationEntityType,
  fields: AutomationFieldPatch[] = [],
): AutomationFieldPatch[] {
  const normalizedPatchMap = sanitizeProposalPatchMap(
    entityType,
    Object.fromEntries(fields.map((field) => [field.path, field.after])),
  );

  return fields.map((field) => ({
    ...field,
    after: Object.prototype.hasOwnProperty.call(normalizedPatchMap, field.path) ? normalizedPatchMap[field.path] : field.after,
  }));
}

export function sanitizeProposal(proposal: AutomationProposal): AutomationProposal {
  return {
    ...proposal,
    fields: sanitizeProposalFields(proposal.entityType, proposal.fields),
  };
}

export function sanitizeAutomationRunProposalFields(run: AutomationRun): AutomationRun {
  return {
    ...run,
    proposals: run.proposals.map((proposal) => sanitizeProposal(proposal)),
  };
}
