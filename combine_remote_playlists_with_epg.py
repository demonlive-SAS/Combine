#!/usr/bin/env python3
"""Combine multiple M3U/M3U8 playlists into one valid, grouped playlist."""

import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== CONFIGURATION =====
PLAYLISTS = [
    "https://github.com/demonlive-SAS/Private-m3u-2/raw/refs/heads/main/Playlists/300rip-wo-events.m3u8",
    "https://github.com/demonlive-SAS/Private-m3u-2/raw/refs/heads/main/Playlists/pxm.m3u", 
    "https://github.com/demonlive-SAS/Private-m3u-2/raw/refs/heads/main/Playlists/s247.m3u",
    "https://github.com/demonlive-SAS/Private-m3u-2/raw/refs/heads/main/Playlists/dancetv.m3u8",
    "https://github.com/demonlive-SAS/Private-m3u-2/raw/refs/heads/main/Playlists/direct.m3u8",
    "https://github.com/demonlive-SAS/buddy_lg/raw/refs/heads/main/lg_channels_us.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_us.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_gb.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_nz.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_mx.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_es.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_ca.m3u",
    "https://github.com/demonlive-SAS/plex-fast-channels/raw/refs/heads/main/playlists/plex_au.m3u",
    "https://github.com/demonlive-SAS/pluto-test/raw/refs/heads/main/pluto_us.m3u",
    "https://github.com/demonlive-SAS/pluto-test/raw/refs/heads/main/pluto_mx.m3u",
    "https://github.com/demonlive-SAS/pluto-test/raw/refs/heads/main/pluto_es.m3u",
    "https://github.com/demonlive-SAS/pluto-test/raw/refs/heads/main/pluto_gb.m3u",
    "https://github.com/demonlive-SAS/pluto-test/raw/refs/heads/main/pluto_ca.m3u",
    "https://github.com/demonlive-SAS/RakutenTV-UK/raw/refs/heads/main/playlist.m3u", 
    "https://github.com/demonlive-SAS/buddy_roku/raw/refs/heads/main/roku.m3u",
    "https://github.com/demonlive-SAS/samstvplus/raw/refs/heads/main/output/samsung_tvplus.m3u", 
    "https://github.com/demonlive-SAS/buddy_tcl_playlist/raw/refs/heads/main/tcl.m3u8",
    "https://github.com/demonlive-SAS/buddy_tubi/raw/refs/heads/main/tubi_playlist.m3u",
    "https://github.com/demonlive-SAS/buddy_xumo/raw/refs/heads/main/playlists/xumo_playlist.m3u",
    "https://github.com/demonlive-SAS/fast-bally/raw/refs/heads/main/ballys.m3u", 
]

EPG_URL = "https://github.com/demonlive-SAS/epg-light/raw/refs/heads/main/epgs/light-epg.xml.gz"
OUTPUT_FILE = "combined_playlist.m3u"

PREFIX_GROUP_WITH_SOURCE = False  # True -> group-title="plex_us | News"
DEDUPE_BY_URL = True              # drop channels whose stream URL was already added
MAX_WORKERS = 8
TIMEOUT = 15

# ===== HELPERS =====
EXTINF_RE = re.compile(r"^(#EXTINF:\s*-?\d+(?:\.\d+)?)(.*)$", re.IGNORECASE)
GROUP_ATTR_RE = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)


def make_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "playlist-combiner/2.0"
    return session


def get_playlist_name(url):
    name = url.rstrip("/").split("/")[-1]
    name = re.sub(r"\.(m3u8?|txt)$", "", name, flags=re.IGNORECASE).strip()
    return name or "Unnamed_Playlist"


def fetch_playlist(session, url):
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        return resp.text.splitlines()
    except requests.RequestException as e:
        print(f"❌ Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def set_group(extinf, group):
    """Return the #EXTINF line with group-title set to `group`."""
    if GROUP_ATTR_RE.search(extinf):
        return GROUP_ATTR_RE.sub(f'group-title="{group}"', extinf, count=1)
    m = EXTINF_RE.match(extinf)
    if m:
        return f'{m.group(1)} group-title="{group}"{m.group(2)}'
    return extinf


def parse_playlist(lines):
    """Yield (group, extinf, extra_tag_lines, url) for each channel."""
    channels = []
    sticky_group = None  # from #EXTGRP
    extinf = None
    extras = []

    for raw in lines:
        line = raw.strip()
        if not line or line.upper().startswith("#EXTM3U"):
            continue

        if line.upper().startswith("#EXTGRP:"):
            sticky_group = line.split(":", 1)[1].strip() or None
            continue

        if line.upper().startswith("#EXTINF"):
            extinf, extras = line, []
            continue

        if line.startswith("#"):
            # #EXTVLCOPT, #KODIPROP, etc. belong to the pending channel
            if extinf is not None:
                extras.append(line)
            continue

        # Non-comment line = stream URL
        if extinf is not None:
            m = GROUP_ATTR_RE.search(extinf)
            group = (m.group(1).split(",")[0].strip() if m else "") or sticky_group or "Ungrouped"
            channels.append((group, extinf, extras, line))
        extinf, extras = None, []

    return channels


def build_output(results):
    seen_urls = set()
    out = [
        f'#EXTM3U x-tvg-url="{EPG_URL}"',
        f"# Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
    ]
    total = 0

    for source_name, channels in results:
        groups = {}
        for group, extinf, extras, url in channels:
            if DEDUPE_BY_URL:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
            label = f"{source_name} | {group}" if PREFIX_GROUP_WITH_SOURCE else group
            groups.setdefault(group, []).append((set_group(extinf, label), extras, url))

        for entries in groups.values():
            for extinf, extras, url in entries:
                out.append(extinf)
                out.extend(extras)
                out.append(url)
                total += 1

    return "\n".join(out) + "\n", total


# ===== MAIN =====
def main():
    urls = [u.strip() for u in PLAYLISTS if u and u.strip()]
    print(f"🚀 Combining {len(urls)} playlists...")

    session = make_session()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fetched = list(pool.map(lambda u: fetch_playlist(session, u), urls))

    results = []
    for url, lines in zip(urls, fetched):  # order preserved
        if not lines:
            continue
        name = get_playlist_name(url)
        channels = parse_playlist(lines)
        print(f"✅ {name}: {len(channels)} channels")
        results.append((name, channels))

    content, total = build_output(results)
    Path(OUTPUT_FILE).write_text(content, encoding="utf-8")

    print(f"\n🎉 Saved {total} channels to '{OUTPUT_FILE}'")
    print(f"📺 EPG: {EPG_URL}")


if __name__ == "__main__":
    main()
