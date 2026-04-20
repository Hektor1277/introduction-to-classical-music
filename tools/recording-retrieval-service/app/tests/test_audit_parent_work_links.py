from __future__ import annotations

import asyncio

import httpx

from scripts.audit_parent_work_links import audit_link


class ApplePageTransport(httpx.MockTransport):
    def __init__(self) -> None:
        super().__init__(self._handler)

    async def _handler(self, request: httpx.Request) -> httpx.Response:
        if "music.apple.com" in str(request.url):
            return httpx.Response(
                200,
                text="""
                <html>
                  <head>
                    <meta property="og:title" content="Schumann: Piano Concerto in A Minor, Op. 54" />
                    <meta property="og:description" content="Martha Argerich, Riccardo Chailly, Royal Concertgebouw Orchestra" />
                  </head>
                </html>
                """,
            )
        return httpx.Response(404, text="not found")


def test_audit_link_supports_apple_music_platform() -> None:
    client = httpx.AsyncClient(transport=ApplePageTransport(), follow_redirects=True)

    payload = asyncio.run(
        audit_link(
            client,
            platform="apple-music",
            url="https://music.apple.com/us/album/demo/123456789?i=987654321&uo=4",
            drafts=[],
        )
    )

    assert payload["available"] is True
    assert payload["resolvedCanonical"] == "apple_music:/us/album/demo/123456789?i=987654321"
    assert payload["title"] == "Schumann: Piano Concerto in A Minor, Op. 54"
    assert payload["auditStatus"] == "available_but_unscored"
