# HTTP/2 HLS Bridge

This plugin provides a local HLS URL for Dispatcharr while fetching the
upstream HLS playlist and media segments over HTTP/2.

## Local stream URL

http://127.0.0.1:8765/playlist.m3u8

## FFmpeg parameters

-i {streamUrl} -map 0 -c copy -f mpegts pipe:1
