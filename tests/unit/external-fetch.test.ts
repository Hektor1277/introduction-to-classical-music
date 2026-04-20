import { describe, expect, it, vi } from "vitest";

import { buildPowershellEncodedCommand, fetchWithWindowsFallback } from "@/lib/external-fetch";

describe("external fetch fallback", () => {
  it("encodes PowerShell scripts as UTF-16LE base64 for -EncodedCommand", () => {
    const script = "$ProgressPreference = 'SilentlyContinue'\nWrite-Output 'ok'";
    const encoded = buildPowershellEncodedCommand(script);
    const decoded = Buffer.from(encoded, "base64").toString("utf16le");

    expect(decoded).toBe(script);
  });

  it("does not leak literal PowerShell backtick-n separators into the encoded script", () => {
    const script = ["$headers = @{}", "Invoke-WebRequest @params", "$result | ConvertTo-Json"].join("\n");
    const encoded = buildPowershellEncodedCommand(script);
    const decoded = Buffer.from(encoded, "base64").toString("utf16le");

    expect(decoded.includes("`n")).toBe(false);
    expect(decoded.split("\n")).toHaveLength(3);
  });

  it("returns the primary fetch result when the request succeeds", async () => {
    const primary = vi.fn(async () => new Response("ok", { status: 200 }));
    const shell = vi.fn();

    const response = await fetchWithWindowsFallback("https://example.com", {}, {
      fetchImpl: primary as typeof fetch,
      powershellGetText: shell,
      platform: "win32",
    });

    expect(await response.text()).toBe("ok");
    expect(primary).toHaveBeenCalledTimes(1);
    expect(shell).not.toHaveBeenCalled();
  });

  it("falls back to PowerShell on Windows GET timeout-like failures", async () => {
    const primary = vi.fn(async () => {
      const error = new Error("fetch failed");
      (error as Error & { cause?: { code?: string; message?: string } }).cause = {
        code: "UND_ERR_CONNECT_TIMEOUT",
        message: "Connect Timeout Error",
      };
      throw error;
    });
    const shell = vi.fn(async () => ({
      status: 200,
      url: "https://example.com/final",
      body: "<html>fallback</html>",
      headers: { "content-type": "text/html; charset=utf-8" },
    }));

    const response = await fetchWithWindowsFallback("https://example.com", {}, {
      fetchImpl: primary as typeof fetch,
      powershellGetText: shell,
      platform: "win32",
    });

    expect(shell).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("fallback");
  });

  it("does not fall back for non-text request bodies", async () => {
    const primary = vi.fn(async () => {
      const error = new Error("fetch failed");
      (error as Error & { cause?: { code?: string } }).cause = { code: "UND_ERR_CONNECT_TIMEOUT" };
      throw error;
    });
    const shell = vi.fn();

    const body = new FormData();
    body.set("hello", "world");

    await expect(
      fetchWithWindowsFallback(
        "https://example.com",
        { method: "POST", body },
        {
          fetchImpl: primary as typeof fetch,
          powershellGetText: shell,
          platform: "win32",
        },
      ),
    ).rejects.toThrow("fetch failed");

    expect(shell).not.toHaveBeenCalled();
  });
  it("falls back to PowerShell on Windows EACCES fetch failures", async () => {
    const primary = vi.fn(async () => {
      const error = new Error("fetch failed");
      (error as Error & { cause?: { code?: string; message?: string } }).cause = {
        code: "EACCES",
        message: "network access denied",
      };
      throw error;
    });
    const shell = vi.fn(async () => ({
      status: 200,
      url: "https://example.com/final",
      body: "fallback-from-eacces",
      headers: { "content-type": "text/plain; charset=utf-8" },
    }));

    const response = await fetchWithWindowsFallback("https://example.com", {}, {
      fetchImpl: primary as typeof fetch,
      powershellGetText: shell,
      platform: "win32",
    });

    expect(shell).toHaveBeenCalledTimes(1);
    expect(await response.text()).toContain("fallback-from-eacces");
  });

  it("falls back to PowerShell for JSON POST requests when the primary fetch is blocked", async () => {
    const primary = vi.fn(async () => {
      const error = new Error("fetch failed");
      (error as Error & { cause?: { code?: string; message?: string } }).cause = {
        code: "EACCES",
        message: "network access denied",
      };
      throw error;
    });
    const shell = vi.fn(async () => ({
      status: 200,
      url: "https://api.example.com/v1/chat/completions",
      body: JSON.stringify({
        ok: true,
        choices: [{ message: { content: "OK" } }],
      }),
      headers: { "content-type": "application/json; charset=utf-8" },
    }));

    const response = await fetchWithWindowsFallback(
      "https://api.example.com/v1/chat/completions",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: "test-model" }),
      },
      {
        fetchImpl: primary as typeof fetch,
        powershellInvokeRequest: shell,
        platform: "win32",
      },
    );

    expect(shell).toHaveBeenCalledTimes(1);
    expect(await response.json()).toMatchObject({ ok: true });
  });
});



