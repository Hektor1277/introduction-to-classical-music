function hashValue(value: string) {
  let hash = 2166136261;

  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return hash >>> 0;
}

export function pickDailyRecommendations<T extends { id: string }>(items: T[], dateKey: string, count: number) {
  return [...items]
    .sort((left, right) => {
      const leftHash = hashValue(`${dateKey}:${left.id}`);
      const rightHash = hashValue(`${dateKey}:${right.id}`);
      return leftHash - rightHash;
    })
    .slice(0, Math.max(0, Math.min(count, items.length)));
}
