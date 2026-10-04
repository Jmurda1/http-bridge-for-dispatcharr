import asyncio
import logging
import re
import threading
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx
from aiohttp import web


class H2HLSBridge:
    def __init__(self, settings, logger):
        self.settings = settings
        self.logger = logger
        self.loop = None
        self.thread = None
        self.runner = None
        self.site = None
        self.client = None
        self.stop_event = threading.Event()

    @property
    def host(self):
        return self.settings.get("listen_host", "0.0.0.0")

    @property
    def port(self):
        return int(self.settings.get("listen_port", 8765))

    @property
    def upstream_url(self):
        return self.settings.get("upstream_url", "").strip()

    def request_headers(self):
        return {
            "Referer": self.settings.get(
                "referer",
                "https://tvnow.st/",
            ),
            "Origin": self.settings.get(
                "origin",
                "https://tvnow.st/",
            ),
            "User-Agent": self.settings.get(
                "user_agent",
                "Mozilla/5.0",
            ),
        }

    def start(self):
        if self.thread and self.thread.is_alive():
            self.logger.info("HTTP/2 HLS bridge is already running")
            return

        if not self.upstream_url:
            raise ValueError("Upstream HLS URL is empty")

        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._run_thread,
            name="h2-hls-bridge",
            daemon=True,
        )
        self.thread.start()

        self.logger.info(
            "HTTP/2 HLS bridge starting on http://%s:%s",
            self.host,
            self.port,
        )

    def stop(self):
        self.stop_event.set()

        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._shutdown(),
                self.loop,
            )
            try:
                future.result(timeout=10)
            except Exception:
                self.logger.exception("Error shutting down HLS bridge")

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)

        self.thread = None
        self.loop = None
        self.logger.info("HTTP/2 HLS bridge stopped")

    def _run_thread(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            self.loop.run_until_complete(self._startup())
            self.loop.run_forever()
        except Exception:
            self.logger.exception("HTTP/2 HLS bridge crashed")
        finally:
            self.loop.run_until_complete(self._shutdown())
            self.loop.close()

    async def _startup(self):
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            headers=self.request_headers(),
            timeout=httpx.Timeout(
                connect=10,
                read=20,
                write=20,
                pool=20,
            ),
        )

        app = web.Application()
        app.router.add_get("/health", self.health)
        app.router.add_get("/playlist.m3u8", self.playlist)
        app.router.add_get("/resource", self.resource)

        self.runner = web.AppRunner(app)
        await self.runner.setup()

        self.site = web.TCPSite(
            self.runner,
            self.host,
            self.port,
        )
        await self.site.start()

        self.logger.info(
            "HLS bridge listening on http://%s:%s",
            self.host,
            self.port,
        )

    async def _shutdown(self):
        if self.client:
            await self.client.aclose()
            self.client = None

        if self.runner:
            await self.runner.cleanup()
            self.runner = None

        self.site = None

    async def health(self, request):
        return web.json_response(
            {
                "status": "ok",
                "http2": True,
                "upstream_configured": bool(self.upstream_url),
            }
        )

    def allowed_url(self, url):
        configured = urlparse(self.upstream_url)
        target = urlparse(url)

        return (
            target.scheme in ("http", "https")
            and target.netloc == configured.netloc
        )

    def proxy_url(self, absolute_url):
        return "/resource?url=" + quote(absolute_url, safe="")

    def rewrite_playlist(self, text, playlist_url):
        output = []

        for line in text.splitlines():
            stripped = line.strip()

            if not stripped:
                output.append("")
                continue

            if stripped.startswith("#"):
                def replace_uri(match):
                    original = urljoin(
                        playlist_url,
                        match.group(1),
                    )
                    return 'URI="' + self.proxy_url(original) + '"'

                line = re.sub(
                    r'URI="([^"]+)"',
                    replace_uri,
                    line,
                )
                output.append(line)
            else:
                absolute = urljoin(playlist_url, stripped)
                output.append(self.proxy_url(absolute))

        return "\n".join(output) + "\n"

    async def playlist(self, request):
        response = await self.client.get(self.upstream_url)
        response.raise_for_status()

        body = self.rewrite_playlist(
            response.text,
            str(response.url),
        )

        return web.Response(
            text=body,
            content_type="application/vnd.apple.mpegurl",
        )

    async def resource(self, request):
        encoded_url = request.query.get("url")

        if not encoded_url:
            raise web.HTTPBadRequest(text="Missing url")

        target_url = unquote(encoded_url)

        if not self.allowed_url(target_url):
            raise web.HTTPForbidden(text="Upstream host is not allowed")

        response = await self.client.get(target_url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        is_playlist = (
            ".m3u8" in content_type.lower()
            or target_url.lower().split("?", 1)[0].endswith(".m3u8")
        )

        if is_playlist:
            body = self.rewrite_playlist(
                response.text,
                str(response.url),
            )

            return web.Response(
                text=body,
                content_type="application/vnd.apple.mpegurl",
            )

        return web.Response(
            body=response.content,
            content_type=(
                content_type.split(";", 1)[0]
                or "application/octet-stream"
            ),
        )


class Plugin:
    name = "HTTP/2 HLS Bridge"
    version = "1.0.0"
    description = (
        "Provides a local HLS URL while fetching upstream playlists "
        "and segments over HTTP/2."
    )
    author = "Local"

    fields = [
        {
            "id": "listen_host",
            "label": "Listen host",
            "type": "string",
            "default": "0.0.0.0",
        },
        {
            "id": "listen_port",
            "label": "Listen port",
            "type": "number",
            "default": 8765,
        },
        {
            "id": "upstream_url",
            "label": "Upstream HLS URL",
            "type": "string",
            "default": "",
        },
        {
            "id": "referer",
            "label": "Referer",
            "type": "string",
            "default": "https://tvnow.st/",
        },
        {
            "id": "origin",
            "label": "Origin",
            "type": "string",
            "default": "https://tvnow.st/",
        },
        {
            "id": "user_agent",
            "label": "User-Agent",
            "type": "string",
            "default": "Mozilla/5.0",
        },
    ]

    actions = [
        {
            "id": "start",
            "label": "Start bridge",
            "button_label": "Start",
        },
        {
            "id": "stop",
            "label": "Stop bridge",
            "button_label": "Stop",
        },
    ]

    def __init__(self):
        self.bridge = None

    def run(self, action, params, context):
        logger = context.get("logger") or logging.getLogger(__name__)

        if action == "start":
            settings = context.get("settings", {})
            self.bridge = H2HLSBridge(settings, logger)
            self.bridge.start()

            return {
                "status": "ok",
                "message": (
                    "Bridge started. Use "
                    "http://127.0.0.1:%s/playlist.m3u8"
                    % self.bridge.port
                ),
            }

        if action == "stop":
            if self.bridge:
                self.bridge.stop()
                self.bridge = None

            return {
                "status": "ok",
                "message": "Bridge stopped",
            }

        return {
            "status": "error",
            "message": "Unknown action: %s" % action,
        }

    def stop(self, context):
        if self.bridge:
            self.bridge.stop()
            self.bridge = None
