import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

type PowershellFetchResult = {
  status: number;
  url: string;
  body: string;
  headers?: Record<string, string>;
};

type PowershellRequestOptions = {
  method?: string;
  headers?: Record<string, string>;
  timeoutMs?: number;
  bodyText?: string;
};

type FetchFallbackOptions = {
  fetchImpl?: typeof fetch;
  powershellGetText?: (url: string, options: { headers: Record<string, string>; timeoutMs: number }) => Promise<PowershellFetchResult>;
  powershellInvokeRequest?: (url: string, options: PowershellRequestOptions) => Promise<PowershellFetchResult>;
  platform?: NodeJS.Platform;
  timeoutMs?: number;
};

export function buildPowershellEncodedCommand(script: string) {
  return Buffer.from(String(script || ""), "utf16le").toString("base64");
}

function normalizeHeaders(headers?: HeadersInit) {
  const normalized: Record<string, string> = {};
  if (!headers) {
    return normalized;
  }

  if (headers instanceof Headers) {
    headers.forEach((value, key) => {
      normalized[key] = value;
    });
    return normalized;
  }

  if (Array.isArray(headers)) {
    headers.forEach(([key, value]) => {
      normalized[String(key)] = String(value);
    });
    return normalized;
  }

  Object.entries(headers).forEach(([key, value]) => {
    if (value !== undefined) {
      normalized[String(key)] = String(value);
    }
  });
  return normalized;
}

function normalizeMethod(init?: RequestInit) {
  return String(init?.method || "GET").toUpperCase();
}

function getTextBody(init?: RequestInit) {
  if (typeof init?.body === "string") {
    return init.body;
  }
  if (init?.body instanceof URLSearchParams) {
    return init.body.toString();
  }
  return undefined;
}

function canFallbackWithPowershell(init?: RequestInit) {
  const method = normalizeMethod(init);
  if (method === "GET" || method === "HEAD") {
    return true;
  }
  return typeof getTextBody(init) === "string";
}

function isRetryableNetworkError(error: unknown) {
  if (!(error instanceof Error)) {
    return false;
  }

  const cause = error.cause && typeof error.cause === "object" ? (error.cause as Record<string, unknown>) : null;
  const code = String(cause?.code ?? "").toUpperCase();
  const message = `${error.message} ${String(cause?.message ?? "")}`.toUpperCase();

  return ["UND_ERR_CONNECT_TIMEOUT", "ETIMEDOUT", "ECONNRESET", "ENETUNREACH", "EHOSTUNREACH", "ECONNREFUSED", "EACCES"].some((token) => code.includes(token) || message.includes(token)) || message.includes("FETCH FAILED");
}

export async function powershellInvokeRequest(url: string, options: PowershellRequestOptions = {}) {
  const headersJson = JSON.stringify(options.headers ?? {});
  const timeoutSec = Math.max(5, Math.ceil((options.timeoutMs ?? 15000) / 1000));
  const bodyBase64 = options.bodyText ? Buffer.from(options.bodyText, "utf8").toString("base64") : "";
  const script = [
    "$ProgressPreference = 'SilentlyContinue'",
    "$headers = @{}",
    "if ($env:CLASSICAL_FETCH_HEADERS_JSON) {",
    "  $headers = $env:CLASSICAL_FETCH_HEADERS_JSON | ConvertFrom-Json -AsHashtable",
    "}",
    "$body = $null",
    "if ($env:CLASSICAL_FETCH_BODY_B64) {",
    "  $body = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($env:CLASSICAL_FETCH_BODY_B64))",
    "}",
    "$params = @{",
    "  UseBasicParsing = $true",
    "  Uri = $env:CLASSICAL_FETCH_URL",
    "  Method = $env:CLASSICAL_FETCH_METHOD",
    "  TimeoutSec = [int]$env:CLASSICAL_FETCH_TIMEOUT_SEC",
    "  Headers = $headers",
    "}",
    "if ($null -ne $body) { $params['Body'] = $body }",
    "$response = Invoke-WebRequest @params",
    "$result = [pscustomobject]@{",
    "  status = [int]$response.StatusCode",
    "  url = $response.BaseResponse.ResponseUri.AbsoluteUri",
    "  body = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($response.Content))",
    "  headers = @{ 'content-type' = [string]$response.Headers['Content-Type'] }",
    "}",
    "$result | ConvertTo-Json -Compress -Depth 6",
  ].join("\n");
  const encodedCommand = buildPowershellEncodedCommand(script);

  const { stdout } = await execFileAsync(
    "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
    ["-NoProfile", "-EncodedCommand", encodedCommand],
    {
      env: {
        ...process.env,
        CLASSICAL_FETCH_URL: url,
        CLASSICAL_FETCH_METHOD: String(options.method || "GET").toUpperCase(),
        CLASSICAL_FETCH_HEADERS_JSON: headersJson,
        CLASSICAL_FETCH_BODY_B64: bodyBase64,
        CLASSICAL_FETCH_TIMEOUT_SEC: String(timeoutSec),
      },
      maxBuffer: 4 * 1024 * 1024,
      windowsHide: true,
    },
  );

  const payload = JSON.parse(String(stdout || "{}").trim()) as {
    status?: number;
    url?: string;
    body?: string;
    headers?: Record<string, string>;
  };

  return {
    status: Number(payload.status || 200),
    url: payload.url || url,
    body: Buffer.from(String(payload.body || ""), "base64").toString("utf8"),
    headers: payload.headers || {},
  } satisfies PowershellFetchResult;
}

export async function powershellGetText(url: string, options: { headers?: Record<string, string>; timeoutMs?: number } = {}) {
  return powershellInvokeRequest(url, {
    method: "GET",
    headers: options.headers,
    timeoutMs: options.timeoutMs,
  });
}

export async function fetchWithWindowsFallback(input: RequestInfo | URL, init: RequestInit = {}, options: FetchFallbackOptions = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  try {
    return await fetchImpl(input, init);
  } catch (error) {
    if ((options.platform ?? process.platform) !== "win32" || !canFallbackWithPowershell(init) || !isRetryableNetworkError(error)) {
      throw error;
    }

    const fallback = options.powershellInvokeRequest ?? (async (url: string, requestOptions: PowershellRequestOptions) => {
      if ((requestOptions.method || "GET").toUpperCase() === "GET" && !requestOptions.bodyText && options.powershellGetText) {
        return options.powershellGetText(url, {
          headers: requestOptions.headers || {},
          timeoutMs: requestOptions.timeoutMs || 15000,
        });
      }
      return powershellInvokeRequest(url, requestOptions);
    });

    const result = await fallback(String(input), {
      method: normalizeMethod(init),
      headers: normalizeHeaders(init.headers),
      timeoutMs: options.timeoutMs ?? 15000,
      bodyText: getTextBody(init),
    });

    return new Response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  }
}


