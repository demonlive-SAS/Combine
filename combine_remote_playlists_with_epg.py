#!/usr/bin/env python3
"""Combine multiple M3U/M3U8 playlists, reorganize by tvg-name, write one playlist.

Pipeline: fetch -> parse -> combine (+dedupe) -> reorganize (sort) -> write
"""

import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== CONFIGURATION =====
PLAYLISTS = [
    "https://github.com/demonlive-SAS/Combine/raw/refs/heads/main/300rip-wo-events-sorted.m3u8",
    "https://github.com/demonlive-SAS/Combine/raw/refs/heads/main/pxm.m3u",
    "https://github.com/demonlive-SAS/Combine/raw/refs/heads/main/liveevents.m3u8",
]

EPG_URL = "https://github.com/demonlive-SAS/epg-light/raw/refs/heads/main/epgs/light-epg.xml.gz"
OUTPUT_FILE = "combined_playlist.m3u"

PREFIX_GROUP_WITH_SOURCE = False  # True -> group-title="plex_us | News"
DEDUPE_BY_URL = True              # drop channels whose stream URL was already added
MAX_WORKERS = 8
TIMEOUT = 15

# --- Reorganization options ---
REORGANIZE = True                 # sort the combined list after merging
SORT_MODE = "name"                # "group_then_name" | "name" (flat A-Z by tvg-name)
GROUP_PRIORITY = []               # e.g. ["News", "Sports"] -> these groups come first, in this order
NATURAL_SORT = True               # "ESPN 2" before "ESPN 10"
FILL_MISSING_TVG_NAME = True      # add tvg-name (from display name) when a channel lacks one

# ===== HELPERS =====
EXTINF_RE = re.compile(r"^(#EXTINF:\s*-?\d+(?:\.\d+)?)(.*)$", re.IGNORECASE)
GROUP_ATTR_RE = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)
TVG_NAME_RE = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)
DIGITS_RE = re.compile(r"(\d+)")


@dataclass
class Channel:
    source: str
    group: str
    extinf: str
    url: str
    extras: list = field(default_factory=list)

    @property
    def tvg_name(self):
        m = TVG_NAME_RE.search(self.extinf)
        return m.group(1).strip() if m else ""

    @property
    def display_name(self):
        return display_name_from_extinf(self.extinf)


def make_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "playlist-combiner/3.0"
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


def display_name_from_extinf(extinf):
    """Text after the last comma that is not inside quotes."""
    in_quotes = False
    last_comma = -1
    for i, ch in enumerate(extinf):
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == "," and not in_quotes:
            last_comma = i
    return extinf[last_comma + 1:].strip() if last_comma >= 0 else ""


def set_group(extinf, group):
    """Return the #EXTINF line with group-title set to `group`."""
    if GROUP_ATTR_RE.search(extinf):
        return GROUP_ATTR_RE.sub(lambda _: f'group-title="{group}"', extinf, count=1)
    m = EXTINF_RE.match(extinf)
    if m:
        return f'{m.group(1)} group-title="{group}"{m.group(2)}'
    return extinf


def set_tvg_name(extinf, name):
    """Add tvg-name=... right after the duration if it is missing."""
    if TVG_NAME_RE.search(extinf):
        return extinf
    m = EXTINF_RE.match(extinf)
    if m:
        safe = name.replace('"', "'")
        return f'{m.group(1)} tvg-name="{safe}"{m.group(2)}'
    return extinf


def parse_playlist(lines, source_name):
    """Return a list of Channel objects parsed from playlist lines."""
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
            channels.append(Channel(source_name, group, extinf, line, extras))
        extinf, extras = None, []

    return channels


# ===== COMBINE =====
def combine(results):
    """Flatten all sources into one list, dropping duplicate stream URLs."""
    seen_urls = set()
    combined = []
    for _source, channels in results:
        for ch in channels:
            if DEDUPE_BY_URL:
                if ch.url in seen_urls:
                    continue
                seen_urls.add(ch.url)
            combined.append(ch)
    return combined


# ===== REORGANIZE =====
def text_key(text):
    text = text.casefold().strip()
    if not NATURAL_SORT:
        return (text,)
    # re.split with a capture group alternates str, digits, str, ... so types always align
    return tuple(int(p) if p.isdigit() else p for p in DIGITS_RE.split(text))


def name_key(ch):
    # Channels with a name sort first; nameless ones go last
    name = ch.tvg_name or ch.display_name
    return (name == "", text_key(name))


def group_key(ch):
    try:
        rank = GROUP_PRIORITY.index(ch.group)
    except ValueError:
        rank = len(GROUP_PRIORITY)
    # Priority groups first (in listed order), then the rest A-Z
    return (rank, text_key(ch.group) if rank == len(GROUP_PRIORITY) else ())


def reorganize(channels):
    """Sort channels by tvg-name (stable, so ties keep source order)."""
    if SORT_MODE == "name":
        return sorted(channels, key=name_key)
    return sorted(channels, key=lambda ch: (group_key(ch), name_key(ch)))


# ===== OUTPUT =====
def build_output(channels):
    out = [
        f'#EXTM3U x-tvg-url="{EPG_URL}"',
        f"# Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
    ]
    for ch in channels:
        extinf = ch.extinf
        if PREFIX_GROUP_WITH_SOURCE:
            extinf = set_group(extinf, f"{ch.source} | {ch.group}")
        elif not GROUP_ATTR_RE.search(extinf):
            extinf = set_group(extinf, ch.group)  # materialize #EXTGRP / "Ungrouped"
        if FILL_MISSING_TVG_NAME and not ch.tvg_name and ch.display_name:
            extinf = set_tvg_name(extinf, ch.display_name)

        out.append(extinf)
        out.extend(ch.extras)
        out.append(ch.url)

    return "\n".join(out) + "\n"


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
        channels = parse_playlist(lines, name)
        print(f"✅ {name}: {len(channels)} channels")
        results.append((name, channels))

    combined = combine(results)
    print(f"🔗 Combined: {len(combined)} channels after dedupe")

    if REORGANIZE:
        combined = reorganize(combined)
        print(f"🔤 Reorganized by tvg-name (mode: {SORT_MODE})")

    Path(OUTPUT_FILE).write_text(build_output(combined), encoding="utf-8")

    print(f"\n🎉 Saved {len(combined)} channels to '{OUTPUT_FILE}'")
    print(f"📺 EPG: {EPG_URL}")


if __name__ == "__main__":
    main()
