import asyncio
import json
import logging
import re
import threading
import time
from urllib.parse import quote, urljoin, urlparse

import httpx
from aiohttp import web


class H2HLSBridge:
    def __init__(self, settings, logger):
        self.settings = settings or {}
        self.logger = logger or logging.getLogger(__name__)

        self.loop = None
        self.thread = None
        self.runner = None
        self.site = None
        self.client = None

        self.channels = {}
        self.start_error = None

        # Live HLS playlists are cached for three seconds.
        self.playlist_cache = {}
        self.playlist_cache_ttl = 3

    @property
    def host(self):
        return str(
            self.settings.get("listen_host", "0.0.0.0")
        ).strip() or "0.0.0.0"

    @property
    def port(self):
        return int(self.settings.get("listen_port", 8765))

    def load_channels(self):
        raw_channels = self.settings.get("channels", "[]")

        if isinstance(raw_channels, list):
            channel_data = raw_channels
        else:
            try:
                channel_data = json.loads(raw_channels or "[]")
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid Protected channels JSON: {exc}"
                ) from exc

        if not isinstance(channel_data, list):
            raise ValueError(
                "Protected channels JSON must be a JSON array"
            )

        loaded = {}

        for item in channel_data:
            if not isinstance(item, dict):
                raise ValueError(
                    "Each channel must be a JSON object"
                )

            channel_id = str(item.get("id", "")).strip()
            upstream_url = str(
                item.get("upstream_url", "")
            ).strip()

            if not channel_id:
                raise ValueError(
                    "Every channel requires an id"
                )

            if not re.fullmatch(
                r"[A-Za-z0-9_-]+",
                channel_id,
            ):
                raise ValueError(
                    f"Invalid channel id: {channel_id}. "
                    "Use only letters, numbers, hyphens, "
                    "and underscores."
                )

            if channel_id in loaded:
                raise ValueError(
                    f"Duplicate channel id: {channel_id}"
                )

            parsed = urlparse(upstream_url)

            if (
                parsed.scheme not in ("http", "https")
                or not parsed.netloc
            ):
                raise ValueError(
                    f"Invalid upstream_url for channel {channel_id}"
                )

            loaded[channel_id] = {
                "id": channel_id,
                "name": str(
                    item.get("name", channel_id)
                ),
                "upstream_url": upstream_url,
                "referer": str(
                    item.get("referer", "")
                ).strip(),
                "origin": str(
                    item.get("origin", "")
                ).strip(),
                "user_agent": str(
                    item.get(
                        "user_agent",
                        "Mozilla/5.0",
                    )
                ).strip(),
                "allowed_hosts": {
                    parsed.netloc.lower()
                },
            }

        self.channels = loaded

        # Remove cache entries for deleted channels.
        self.playlist_cache = {
            channel_id: cached
            for channel_id, cached
            in self.playlist_cache.items()
            if channel_id in self.channels
        }

    def start(self):
        if self.thread and self.thread.is_alive():
            self.logger.info(
                "H2 HLS bridge is already running"
            )
            return

        self.load_channels()

        if not self.channels:
            raise ValueError(
                "No protected channels are configured"
            )

        self.start_error = None

        self.thread = threading.Thread(
            target=self._run_thread,
            name="h2-hls-bridge",
            daemon=True,
        )
        self.thread.start()

        self.logger.info(
            "H2 HLS bridge starting on %s:%s",
            self.host,
            self.port,
        )

    def stop(self):
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._shutdown(),
                self.loop,
            )

            try:
                future.result(timeout=10)
            except Exception:
                self.logger.exception(
                    "Error stopping H2 HLS bridge"
                )

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)

        self.thread = None
        self.loop = None
        self.runner = None
        self.site = None

        self.logger.info(
            "H2 HLS bridge stopped"
        )

    def _run_thread(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            self.loop.run_until_complete(
                self._startup()
            )
            self.loop.run_forever()

        except Exception as exc:
            self.start_error = exc
            self.logger.exception(
                "H2 HLS bridge crashed"
            )

        finally:
            if self.loop and not self.loop.is_closed():
                try:
                    self.loop.run_until_complete(
                        self._shutdown()
                    )
                except Exception:
                    self.logger.exception(
                        "Error during bridge cleanup"
                    )

                self.loop.close()

    async def _startup(self):
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(
                connect=10,
                read=30,
                write=30,
                pool=30,
            ),
        )

        app = web.Application(
            client_max_size=1024 * 1024 * 20
        )

        app.router.add_get(
            "/health",
            self.health,
        )

        app.router.add_get(
            "/hls/{channel_id}/playlist.m3u8",
            self.playlist,
        )

        app.router.add_get(
            "/hls/{channel_id}/resource",
            self.resource,
        )

        self.runner = web.AppRunner(app)
        await self.runner.setup()

        self.site = web.TCPSite(
            self.runner,
            self.host,
            self.port,
        )

        await self.site.start()

        self.logger.info(
            "H2 HLS bridge listening on %s:%s",
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

    def channel_from_request(self, request):
        channel_id = request.match_info.get(
            "channel_id",
            "",
        )

        channel = self.channels.get(channel_id)

        if not channel:
            raise web.HTTPNotFound(
                text="Unknown bridge channel"
            )

        return channel_id, channel

    def request_headers(self, channel):
        headers = {
            "User-Agent": channel.get(
                "user_agent",
                "Mozilla/5.0",
            )
        }

        referer = channel.get("referer", "")
        origin = channel.get("origin", "")

        if referer:
            headers["Referer"] = referer

        if origin:
            headers["Origin"] = origin

        return headers

    def add_allowed_host(self, url, channel):
        host = urlparse(url).netloc.lower()

        if host:
            channel["allowed_hosts"].add(host)

    def is_allowed_url(self, url, channel):
        parsed = urlparse(url)

        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.netloc)
            and parsed.netloc.lower()
            in channel["allowed_hosts"]
        )

    async def fetch(self, url, channel):
        if not self.is_allowed_url(url, channel):
            raise web.HTTPForbidden(
                text="Upstream host is not allowed"
            )

        try:
            response = await self.client.get(
                url,
                headers=self.request_headers(channel),
            )

            response.raise_for_status()

        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code

            if status == 429:
                retry_after = (
                    exc.response.headers.get(
                        "retry-after"
                    )
                )

                self.logger.warning(
                    "Upstream rate-limited %s; "
                    "Retry-After=%s",
                    url,
                    retry_after,
                )

                raise web.HTTPTooManyRequests(
                    text="Upstream rate limit reached",
                    headers={
                        "Retry-After": (
                            retry_after or "60"
                        ),
                    },
                ) from exc

            self.logger.error(
                "Upstream HTTP %s for %s",
                status,
                url,
            )

            raise web.HTTPBadGateway(
                text=f"Upstream returned HTTP {status}"
            ) from exc

        except httpx.HTTPError as exc:
            self.logger.error(
                "Upstream request failed for %s: %s",
                url,
                exc,
            )

            raise web.HTTPBadGateway(
                text="Unable to fetch upstream stream"
            ) from exc

        self.add_allowed_host(
            str(response.url),
            channel,
        )

        return response

    def local_resource_url(self, channel_id, url):
        return (
            "/hls/"
            + quote(channel_id, safe="")
            + "/resource?url="
            + quote(url, safe="")
        )

    def rewrite_playlist(
        self,
        text,
        playlist_url,
        channel_id,
        channel,
    ):
        output = []

        uri_pattern = re.compile(
            r'(\bURI=")([^"]+)(")',
            re.IGNORECASE,
        )

        for original_line in text.splitlines():
            line = original_line.strip()

            if not line:
                output.append("")
                continue

            if line.startswith("#"):

                def replace_uri(match):
                    original_url = urljoin(
                        playlist_url,
                        match.group(2),
                    )

                    self.add_allowed_host(
                        original_url,
                        channel,
                    )

                    local_url = self.local_resource_url(
                        channel_id,
                        original_url,
                    )

                    return (
                        match.group(1)
                        + local_url
                        + match.group(3)
                    )

                rewritten = uri_pattern.sub(
                    replace_uri,
                    original_line,
                )

                output.append(rewritten)
                continue

            absolute_url = urljoin(
                playlist_url,
                line,
            )

            self.add_allowed_host(
                absolute_url,
                channel,
            )

            output.append(
                self.local_resource_url(
                    channel_id,
                    absolute_url,
                )
            )

        return "\n".join(output) + "\n"

    @staticmethod
    def is_playlist(response, target_url):
        content_type = response.headers.get(
            "content-type",
            "",
        ).lower()

        path = urlparse(
            str(response.url or target_url)
        ).path.lower()

        return (
            "mpegurl" in content_type
            or "vnd.apple.mpegurl" in content_type
            or path.endswith(".m3u8")
            or ".m3u8" in target_url.lower()
        )

    def get_cached_playlist(self, channel_id):
        cached = self.playlist_cache.get(
            channel_id
        )

        if not cached:
            return None

        timestamp, body = cached

        if (
            time.monotonic() - timestamp
            > self.playlist_cache_ttl
        ):
            self.playlist_cache.pop(
                channel_id,
                None,
            )
            return None

        return body

    def cache_playlist(self, channel_id, body):
        self.playlist_cache[channel_id] = (
            time.monotonic(),
            body,
        )

    async def health(self, request):
        return web.json_response(
            {
                "status": "ok",
                "http2_upstream": True,
                "playlist_cache_ttl": (
                    self.playlist_cache_ttl
                ),
                "channels": sorted(
                    self.channels.keys()
                ),
            }
        )

    async def playlist(self, request):
        channel_id, channel = (
            self.channel_from_request(request)
        )

        cached_body = self.get_cached_playlist(
            channel_id
        )

        if cached_body is not None:
            return web.Response(
                text=cached_body,
                content_type=(
                    "application/vnd.apple.mpegurl"
                ),
                headers={
                    "Cache-Control": "no-store",
                    "Access-Control-Allow-Origin": "*",
                },
            )

        response = await self.fetch(
            channel["upstream_url"],
            channel,
        )

        body = self.rewrite_playlist(
            response.text,
            str(response.url),
            channel_id,
            channel,
        )

        self.cache_playlist(
            channel_id,
            body,
        )

        return web.Response(
            text=body,
            content_type=(
                "application/vnd.apple.mpegurl"
            ),
            headers={
                "Cache-Control": "no-store",
                "Access-Control-Allow-Origin": "*",
            },
        )

    async def resource(self, request):
        channel_id, channel = (
            self.channel_from_request(request)
        )

        target_url = request.query.get(
            "url",
            "",
        ).strip()

        if not target_url:
            raise web.HTTPBadRequest(
                text="Missing url parameter"
            )

        response = await self.fetch(
            target_url,
            channel,
        )

        if self.is_playlist(
            response,
            target_url,
        ):
            body = self.rewrite_playlist(
                response.text,
                str(response.url),
                channel_id,
                channel,
            )

            return web.Response(
                text=body,
                content_type=(
                    "application/vnd.apple.mpegurl"
                ),
                headers={
                    "Cache-Control": "no-store",
                    "Access-Control-Allow-Origin": "*",
                },
            )

        content_type = response.headers.get(
            "content-type",
            "",
        ).split(";", 1)[0].strip()

        return web.Response(
            body=response.content,
            content_type=(
                content_type
                or "application/octet-stream"
            ),
            headers={
                "Access-Control-Allow-Origin": "*",
            },
        )


class Plugin:
    name = "HTTP/2 HLS Bridge"
    version = "2.0.1"
    description = (
        "Per-channel HLS proxy for streams requiring "
        "custom Referer, Origin, and User-Agent headers."
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
            "id": "channels",
            "label": "Protected channels JSON",
            "type": "text",
            "default": "[]",
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
        logger = (
            context.get("logger")
            or logging.getLogger(__name__)
        )

        if action == "start":
            if self.bridge:
                self.bridge.stop()
                self.bridge = None

            settings = context.get(
                "settings",
                {},
            )

            self.bridge = H2HLSBridge(
                settings=settings,
                logger=logger,
            )

            try:
                self.bridge.start()

            except Exception as exc:
                self.bridge = None

                return {
                    "status": "error",
                    "message": str(exc),
                }

            return {
                "status": "ok",
                "message": (
                    "Bridge started. Protected channel "
                    "URLs use "
                    "/hls/<channel_id>/playlist.m3u8"
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
            "message": (
                "Unknown action: "
                + str(action)
            ),
        }

    def stop(self, context):
        if self.bridge:
            self.bridge.stop()
            self.bridge = None
