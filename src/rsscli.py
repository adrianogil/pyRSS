#!/usr/bin/env python3
"""
rsscli.py - Minimal RSS reader with:
- SQLite persistence (feeds + entries)
- CLI: add/list/fetch/updates
- Python API: RSSStore.fetch_all(), RSSStore.get_updates_for_day(), RSSStore.fetch_and_get_updates_for_day()

Dependency:
  pip install feedparser
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import feedparser


# -----------------------------
# Utilities
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def parse_date_like(s: str) -> date:
    # Expect YYYY-MM-DD
    return date.fromisoformat(s)

def to_iso_dt(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def entry_guid(entry: Dict[str, Any]) -> str:
    """
    Generate a stable "guid" for dedupe:
    Prefer entry.id/guid; else link; else hash(title+published).
    """
    for k in ("id", "guid"):
        v = entry.get(k)
        if v:
            return str(v).strip()

    link = entry.get("link")
    if link:
        return str(link).strip()

    title = str(entry.get("title", "")).strip()
    published = str(entry.get("published", "")).strip()
    raw = (title + "|" + published).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def normalize_text(v: Any, max_len: Optional[int] = None) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if max_len is not None and len(s) > max_len:
        return s[:max_len]
    return s

def pick_summary(entry: Dict[str, Any]) -> Optional[str]:
    # feedparser may provide summary/detail/content
    for key in ("summary", "subtitle"):
        v = entry.get(key)
        if v:
            return normalize_text(v)
    return None

def pick_content(entry: Dict[str, Any]) -> Optional[str]:
    content = entry.get("content")
    if isinstance(content, list) and content:
        # each item is dict like {"type": "...", "value": "..."}
        val = content[0].get("value")
        return normalize_text(val)
    return None

def parse_entry_datetime(entry: Dict[str, Any]) -> Optional[datetime]:
    """
    Try to use published_parsed or updated_parsed first (struct_time).
    """
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key)
        if st:
            # struct_time in local? feedparser treats it as UTC-ish;
            # we store it as UTC to keep query logic simple.
            return datetime(*st[:6], tzinfo=timezone.utc)
    return None


# -----------------------------
# Data shapes
# -----------------------------

@dataclass(frozen=True)
class Feed:
    id: int
    url: str
    title: Optional[str]

@dataclass(frozen=True)
class Entry:
    id: int
    feed_id: int
    guid: str
    title: Optional[str]
    link: Optional[str]
    author: Optional[str]
    published_at: Optional[str]  # ISO UTC
    summary: Optional[str]
    content: Optional[str]
    fetched_at: str              # ISO UTC


# -----------------------------
# SQLite store / API
# -----------------------------

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS feeds (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  url             TEXT NOT NULL UNIQUE,
  title           TEXT,
  etag            TEXT,
  modified        TEXT,         -- stored as text (feedparser gives tuple/struct-like; we'll store str())
  added_at        TEXT NOT NULL,
  last_checked_at TEXT
);

CREATE TABLE IF NOT EXISTS entries (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  feed_id       INTEGER NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
  guid          TEXT NOT NULL,
  title         TEXT,
  link          TEXT,
  author        TEXT,
  published_at  TEXT,           -- ISO UTC (or NULL)
  summary       TEXT,
  content       TEXT,
  fetched_at    TEXT NOT NULL,  -- ISO UTC

  UNIQUE(feed_id, guid)
);

CREATE INDEX IF NOT EXISTS idx_entries_published_at ON entries(published_at);
CREATE INDEX IF NOT EXISTS idx_entries_fetched_at   ON entries(fetched_at);
"""

class RSSStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)

    # ---- Feeds ----

    def add_feed(self, url: str, title: Optional[str] = None) -> Feed:
        url = url.strip()
        if not url:
            raise ValueError("Feed URL is empty.")

        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO feeds(url, title, added_at) VALUES(?,?,?)",
                (url, title, utc_now_iso()),
            )
            row = conn.execute("SELECT id, url, title FROM feeds WHERE url = ?", (url,)).fetchone()
            if not row:
                raise RuntimeError("Failed to insert/feed lookup.")
            return Feed(id=int(row["id"]), url=str(row["url"]), title=row["title"])

    def list_feeds(self) -> List[Feed]:
        with self._conn() as conn:
            rows = conn.execute("SELECT id, url, title FROM feeds ORDER BY id").fetchall()
            return [Feed(int(r["id"]), str(r["url"]), r["title"]) for r in rows]

    # ---- Fetching ----

    def fetch_all(self) -> Dict[str, Any]:
        """
        Fetch all feeds, store new entries.
        Returns a summary dict.
        """
        feeds = self._get_feed_rows()
        summary = {
            "feeds_total": len(feeds),
            "feeds_fetched": 0,
            "entries_inserted": 0,
            "entries_seen": 0,
            "errors": [],
        }

        for f in feeds:
            try:
                inserted, seen = self._fetch_one_feed(f)
                summary["feeds_fetched"] += 1
                summary["entries_inserted"] += inserted
                summary["entries_seen"] += seen
            except Exception as e:
                summary["errors"].append({"feed_id": f["id"], "url": f["url"], "error": str(e)})

        return summary

    def _get_feed_rows(self) -> List[sqlite3.Row]:
        with self._conn() as conn:
            return conn.execute("SELECT * FROM feeds ORDER BY id").fetchall()

    def _fetch_one_feed(self, feed_row: sqlite3.Row) -> Tuple[int, int]:
        feed_id = int(feed_row["id"])
        url = str(feed_row["url"])

        etag = feed_row["etag"]
        modified = feed_row["modified"]

        # feedparser supports conditional GET via etag/modified.
        # We store modified as a string; feedparser accepts it best as a tuple/struct_time,
        # so we only use etag for conditional GET here to keep things simple.
        # (You can improve this later by storing modified_parsed properly.)
        d = feedparser.parse(url, etag=etag)

        fetched_at = utc_now_iso()
        status = getattr(d, "status", None)

        with self._conn() as conn:
            conn.execute(
                "UPDATE feeds SET last_checked_at = ?, etag = COALESCE(?, etag) WHERE id = ?",
                (fetched_at, getattr(d, "etag", None), feed_id),
            )

            # Store title if missing
            feed_title = getattr(getattr(d, "feed", {}), "title", None) or d.get("feed", {}).get("title")
            if feed_title:
                conn.execute(
                    "UPDATE feeds SET title = COALESCE(title, ?) WHERE id = ?",
                    (normalize_text(feed_title, 500), feed_id),
                )

            # 304 Not Modified -> no entries list changes
            if status == 304:
                return (0, 0)

            inserted = 0
            seen = 0

            for entry in d.get("entries", []):
                seen += 1
                guid = entry_guid(entry)

                title = normalize_text(entry.get("title"), 2000)
                link = normalize_text(entry.get("link"), 4000)
                author = normalize_text(entry.get("author"), 1000)
                summary = pick_summary(entry)
                content = pick_content(entry)

                dt = parse_entry_datetime(entry)
                published_at = to_iso_dt(dt)

                # Insert (dedupe by UNIQUE(feed_id, guid))
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO entries
                    (feed_id, guid, title, link, author, published_at, summary, content, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (feed_id, guid, title, link, author, published_at, summary, content, fetched_at),
                )
                if cur.rowcount == 1:
                    inserted += 1

            return (inserted, seen)

    # ---- Queries ----

    def get_updates_for_day(self, day: date, use_published_at: bool = True) -> List[Entry]:
        """
        Get entries whose published_at date == day (UTC), if available;
        otherwise you may use fetched_at date by setting use_published_at=False.
        """
        day_start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        day_end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).isoformat()

        col = "published_at" if use_published_at else "fetched_at"
        where = f"{col} IS NOT NULL AND {col} BETWEEN ? AND ?"

        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, feed_id, guid, title, link, author, published_at, summary, content, fetched_at
                FROM entries
                WHERE {where}
                ORDER BY COALESCE(published_at, fetched_at) DESC
                """,
                (day_start, day_end),
            ).fetchall()

            return [
                Entry(
                    id=int(r["id"]),
                    feed_id=int(r["feed_id"]),
                    guid=str(r["guid"]),
                    title=r["title"],
                    link=r["link"],
                    author=r["author"],
                    published_at=r["published_at"],
                    summary=r["summary"],
                    content=r["content"],
                    fetched_at=str(r["fetched_at"]),
                )
                for r in rows
            ]

    def fetch_and_get_updates_for_day(self, day: date, use_published_at: bool = True) -> List[Entry]:
        """
        This is the “one call” API you described:
          - fetch new (update DB)
          - then query entries for that day
        """
        self.fetch_all()
        return self.get_updates_for_day(day, use_published_at=use_published_at)


# -----------------------------
# CLI
# -----------------------------

def cmd_add(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    feed = store.add_feed(args.url, title=args.title)
    print(f"Added/exists: id={feed.id} url={feed.url} title={feed.title or ''}")
    return 0

def cmd_list(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    feeds = store.list_feeds()
    if not feeds:
        print("No feeds registered.")
        return 0
    for f in feeds:
        print(f"{f.id}\t{f.url}\t{f.title or ''}")
    return 0

def cmd_fetch(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    s = store.fetch_all()
    print(f"Feeds total:    {s['feeds_total']}")
    print(f"Feeds fetched:  {s['feeds_fetched']}")
    print(f"Entries seen:   {s['entries_seen']}")
    print(f"Entries new:    {s['entries_inserted']}")
    if s["errors"]:
        print("\nErrors:")
        for e in s["errors"]:
            print(f"- feed_id={e['feed_id']} url={e['url']} error={e['error']}")
        return 2
    return 0

def cmd_updates(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    day = parse_date_like(args.date)

    if args.fetch_first:
        entries = store.fetch_and_get_updates_for_day(day, use_published_at=not args.by_fetched)
    else:
        entries = store.get_updates_for_day(day, use_published_at=not args.by_fetched)

    if not entries:
        print("No entries found.")
        return 0

    for e in entries:
        when = e.published_at or e.fetched_at
        title = (e.title or "").replace("\n", " ").strip()
        link = e.link or ""
        print(f"[{when}] feed={e.feed_id} {title}")
        if link:
            print(f"  {link}")
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rsscli", description="SQLite-backed RSS CLI")
    p.add_argument("--db", default=os.path.expanduser("~/.local/share/rsscli/rss.sqlite3"), help="Path to sqlite DB")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="Register a new feed URL")
    p_add.add_argument("url")
    p_add.add_argument("--title", default=None)
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="List registered feeds")
    p_list.set_defaults(func=cmd_list)

    p_fetch = sub.add_parser("fetch", help="Fetch updates from all feeds and store in DB")
    p_fetch.set_defaults(func=cmd_fetch)

    p_updates = sub.add_parser("updates", help="Show updates for a given day (UTC)")
    p_updates.add_argument("--date", required=True, help="YYYY-MM-DD")
    p_updates.add_argument("--fetch-first", action="store_true", help="Fetch before querying")
    p_updates.add_argument("--by-fetched", action="store_true", help="Filter by fetched_at date instead of published_at")
    p_updates.set_defaults(func=cmd_updates)

    return p

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    raise SystemExit(main())
