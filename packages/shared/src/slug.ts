export function createSlug(value: string) {
  const slug = value
    .normalize("NFKD")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[“”"'.·]/g, "")
    .replace(/[\/_,:;()[\]{}]+/g, "-")
    .replace(/[^\p{Letter}\p{Number}-]+/gu, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return slug || "item";
}

export function createEntityId(prefix: string, value: string) {
  return `${prefix}-${createSlug(value)}`;
}

export function createSortKey(index: number) {
  return String((index + 1) * 10).padStart(4, "0");
}

export function ensureUniqueValue(candidate: string, existing: Set<string>) {
  if (!existing.has(candidate)) {
    return candidate;
  }

  let counter = 2;
  while (existing.has(`${candidate}-${counter}`)) {
    counter += 1;
  }

  return `${candidate}-${counter}`;
}
