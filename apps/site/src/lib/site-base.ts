const PROTOCOL_PATTERN = /^[a-zA-Z][a-zA-Z\d+\-.]*:/;

export function normalizeBasePath(value: string | undefined) {
  const trimmed = String(value || "/").trim();
  if (!trimmed || trimmed === "/") {
    return "/";
  }
  const withLeadingSlash = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeadingSlash.endsWith("/") ? withLeadingSlash : `${withLeadingSlash}/`;
}

export function withBasePath(pathname: string, basePath: string | undefined = import.meta.env.BASE_URL) {
  const value = String(pathname || "").trim();
  if (!value) {
    return normalizeBasePath(basePath);
  }
  if (value.startsWith("#") || value.startsWith("?") || value.startsWith("//") || PROTOCOL_PATTERN.test(value)) {
    return value;
  }
  if (!value.startsWith("/")) {
    return value;
  }
  const normalizedBase = normalizeBasePath(basePath);
  if (normalizedBase === "/") {
    return value;
  }
  const normalizedBaseNoTrailingSlash = normalizedBase.slice(0, -1);
  if (value === normalizedBaseNoTrailingSlash || value.startsWith(normalizedBase)) {
    return value;
  }
  if (value === "/") {
    return normalizedBase;
  }
  return `${normalizedBase}${value.slice(1)}`;
}
