#!/usr/bin/env python3
"""
pyrss.py - SQLite-backed RSS reader with:
- Feeds + entries persisted
- Categories for feeds
- Fetch updates and store deduped entries
- Query updates by day or last N days
- Full-text search (FTS5) on title/summary/content (with fallback to LIKE)

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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import feedparser


# -----------------------------
# Utilities
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def utc_today() -> date:
    return datetime.now(timezone.utc).date()

def parse_date_like(s: str) -> date:
    return date.fromisoformat(s)

def to_iso_dt(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def entry_guid(entry: Dict[str, Any]) -> str:
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
    for key in ("summary", "subtitle"):
        v = entry.get(key)
        if v:
            return normalize_text(v)
    return None

def pick_content(entry: Dict[str, Any]) -> Optional[str]:
    content = entry.get("content")
    if isinstance(content, list) and content:
        val = content[0].get("value")
        return normalize_text(val)
    return None

def parse_entry_datetime(entry: Dict[str, Any]) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key)
        if st:
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
    category: str

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
# SQLite schema
# -----------------------------

SCHEMA_BASE_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS feeds (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  url             TEXT NOT NULL UNIQUE,
  title           TEXT,
  category        TEXT NOT NULL DEFAULT 'default',
  etag            TEXT,
  modified        TEXT,
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
  published_at  TEXT,
  summary       TEXT,
  content       TEXT,
  fetched_at    TEXT NOT NULL,

  UNIQUE(feed_id, guid)
);

CREATE INDEX IF NOT EXISTS idx_entries_published_at ON entries(published_at);
CREATE INDEX IF NOT EXISTS idx_entries_fetched_at   ON entries(fetched_at);
CREATE INDEX IF NOT EXISTS idx_feeds_category       ON feeds(category);
"""

SCHEMA_FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
  title,
  summary,
  content,
  entry_id UNINDEXED,
  feed_id  UNINDEXED,
  published_at UNINDEXED,
  fetched_at   UNINDEXED
);

CREATE TRIGGER IF NOT EXISTS entries_ai AFTER INSERT ON entries BEGIN
  INSERT INTO entries_fts(entry_id, feed_id, published_at, fetched_at, title, summary, content)
  VALUES (
    new.id,
    new.feed_id,
    new.published_at,
    new.fetched_at,
    COALESCE(new.title, ''),
    COALESCE(new.summary, ''),
    COALESCE(new.content, '')
  );
END;

CREATE TRIGGER IF NOT EXISTS entries_ad AFTER DELETE ON entries BEGIN
  DELETE FROM entries_fts WHERE entry_id = old.id;
END;

CREATE TRIGGER IF NOT EXISTS entries_au AFTER UPDATE ON entries BEGIN
  DELETE FROM entries_fts WHERE entry_id = old.id;
  INSERT INTO entries_fts(entry_id, feed_id, published_at, fetched_at, title, summary, content)
  VALUES (
    new.id,
    new.feed_id,
    new.published_at,
    new.fetched_at,
    COALESCE(new.title, ''),
    COALESCE(new.summary, ''),
    COALESCE(new.content, '')
  );
END;
"""


# -----------------------------
# SQLite store / API
# -----------------------------

class RSSStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._fts_enabled = False
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
            conn.executescript(SCHEMA_BASE_SQL)

            # Migration for existing DBs that were created before "category"
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(feeds)").fetchall()}
            if "category" not in cols:
                conn.execute("ALTER TABLE feeds ADD COLUMN category TEXT NOT NULL DEFAULT 'default'")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_feeds_category ON feeds(category)")

            # Try enable FTS5
            try:
                conn.executescript(SCHEMA_FTS_SQL)
                self._fts_enabled = True
            except sqlite3.OperationalError:
                self._fts_enabled = False

    # ---- Feeds ----

    def add_feed(self, url: str, category: str = "default", title: Optional[str] = None) -> Feed:
        url = url.strip()
        category = (category or "default").strip() or "default"
        if not url:
            raise ValueError("Feed URL is empty.")

        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO feeds(url, title, category, added_at) VALUES(?,?,?,?)",
                (url, title, category, utc_now_iso()),
            )
            # If it already exists but caller provided a new category, update it
            conn.execute(
                "UPDATE feeds SET category = COALESCE(NULLIF(?, ''), category) WHERE url = ?",
                (category, url),
            )
            row = conn.execute(
                "SELECT id, url, title, category FROM feeds WHERE url = ?",
                (url,),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to insert/feed lookup.")
            return Feed(id=int(row["id"]), url=str(row["url"]), title=row["title"], category=str(row["category"]))

    def list_feeds(self) -> List[Feed]:
        with self._conn() as conn:
            rows = conn.execute("SELECT id, url, title, category FROM feeds ORDER BY category, id").fetchall()
            return [Feed(int(r["id"]), str(r["url"]), r["title"], str(r["category"])) for r in rows]

    def delete_feed(self, *, feed_id: Optional[int] = None, url: Optional[str] = None) -> int:
        if feed_id is None and not url:
            raise ValueError("Provide feed_id or url to delete.")

        with self._conn() as conn:
            if feed_id is not None:
                cur = conn.execute("DELETE FROM feeds WHERE id = ?", (int(feed_id),))
            else:
                cur = conn.execute("DELETE FROM feeds WHERE url = ?", (url.strip(),))
            return int(cur.rowcount)

    # ---- Fetching ----

    def fetch_all(self) -> Dict[str, Any]:
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

        d = feedparser.parse(url, etag=etag)

        fetched_at = utc_now_iso()
        status = getattr(d, "status", None)

        with self._conn() as conn:
            conn.execute(
                "UPDATE feeds SET last_checked_at = ?, etag = COALESCE(?, etag) WHERE id = ?",
                (fetched_at, getattr(d, "etag", None), feed_id),
            )

            feed_title = getattr(getattr(d, "feed", {}), "title", None) or d.get("feed", {}).get("title")
            if feed_title:
                conn.execute(
                    "UPDATE feeds SET title = COALESCE(title, ?) WHERE id = ?",
                    (normalize_text(feed_title, 500), feed_id),
                )

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
        start_dt = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        end_dt = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).isoformat()

        return self.get_updates_between(start_dt, end_dt, use_published_at=use_published_at)

    def get_updates_last_days(self, days: int, use_published_at: bool = True) -> List[Entry]:
        """
        Returns entries from the last N days, inclusive (UTC).
        Example: days=1 => only today (UTC).
                 days=3 => today + previous 2 days (UTC).
        """
        days = int(days)
        if days <= 0:
            return []

        end_day = utc_today()
        start_day = end_day - timedelta(days=days - 1)

        start_dt = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        end_dt = datetime(end_day.year, end_day.month, end_day.day, 23, 59, 59, tzinfo=timezone.utc).isoformat()

        return self.get_updates_between(start_dt, end_dt, use_published_at=use_published_at)

    def get_updates_between(self, start_iso: str, end_iso: str, use_published_at: bool = True) -> List[Entry]:
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
                (start_iso, end_iso),
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
        self.fetch_all()
        return self.get_updates_for_day(day, use_published_at=use_published_at)

    def fetch_and_get_updates_last_days(self, days: int, use_published_at: bool = True) -> List[Entry]:
        self.fetch_all()
        return self.get_updates_last_days(days, use_published_at=use_published_at)

    # ---- Search ----

    def search(self, query: str, limit: int = 50, category: Optional[str] = None) -> List[Entry]:
        q = (query or "").strip()
        if not q:
            return []

        limit = max(1, min(int(limit), 500))

        if self._fts_enabled:
            return self._search_fts(q, limit=limit, category=category)
        return self._search_like(q, limit=limit, category=category)

    def _search_fts(self, q: str, limit: int, category: Optional[str]) -> List[Entry]:
        sql = """
        SELECT
        e.id, e.feed_id, e.guid, e.title, e.link, e.author, e.published_at, e.summary, e.content, e.fetched_at
        FROM entries_fts
        JOIN entries e ON e.id = entries_fts.entry_id
        JOIN feeds   d ON d.id = e.feed_id
        WHERE entries_fts MATCH ?
        {cat_filter}
        ORDER BY bm25(entries_fts) ASC, COALESCE(e.published_at, e.fetched_at) DESC
        LIMIT ?
        """

        cat_filter = ""
        params: List[Any] = [q]
        if category:
            cat_filter = "AND d.category = ?"
            params.append(category)

        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(sql.format(cat_filter=cat_filter), params).fetchall()
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


    def _search_like(self, q: str, limit: int, category: Optional[str]) -> List[Entry]:
        like = f"%{q}%"
        sql = """
        SELECT
          e.id, e.feed_id, e.guid, e.title, e.link, e.author, e.published_at, e.summary, e.content, e.fetched_at
        FROM entries e
        JOIN feeds d ON d.id = e.feed_id
        WHERE (e.title LIKE ? OR e.summary LIKE ?)
        {cat_filter}
        ORDER BY COALESCE(e.published_at, e.fetched_at) DESC
        LIMIT ?
        """

        cat_filter = ""
        params: List[Any] = [like, like]
        if category:
            cat_filter = "AND d.category = ?"
            params.append(category)
        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(sql.format(cat_filter=cat_filter), params).fetchall()
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


# -----------------------------
# CLI
# -----------------------------

def print_entries_grouped_by_day(entries: List[Entry]) -> None:
    """
    Nice output: adds a date header when the day changes.
    Uses published_at if present, else fetched_at.
    """
    last_day: Optional[str] = None
    for e in entries:
        when = e.published_at or e.fetched_at
        day = when[:10] if when else "????-??-??"
        if day != last_day:
            print(f"\n=== {day} ===")
            last_day = day

        title = (e.title or "").replace("\n", " ").strip()
        link = e.link or ""
        print(f"[{when}] feed={e.feed_id} {title}")
        if link:
            print(f"  {link}")

def cmd_add(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    feed = store.add_feed(args.url, category=args.category, title=None)
    print(f"Added/exists: id={feed.id} category={feed.category} url={feed.url} title={feed.title or ''}")
    return 0

def cmd_list(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    feeds = store.list_feeds()
    if not feeds:
        print("No feeds registered.")
        return 0
    for f in feeds:
        print(f"{f.id}\t{f.category}\t{f.url}\t{f.title or ''}")
    return 0

def cmd_delete(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    deleted = store.delete_feed(feed_id=args.id, url=args.url)
    if deleted == 0:
        print("No matching feed found.")
        return 1
    print(f"Deleted {deleted} feed(s).")
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

    use_published = not args.by_fetched

    if args.fetch_first:
        store.fetch_all()

    if args.date:
        day = parse_date_like(args.date)
        entries = store.get_updates_for_day(day, use_published_at=use_published)
    else:
        # default behavior: last N days (default 1)
        entries = store.get_updates_last_days(args.last, use_published_at=use_published)

    if not entries:
        print("No entries found.")
        return 0

    print_entries_grouped_by_day(entries)
    return 0

def cmd_search(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)

    if args.fetch_first:
        store.fetch_all()

    results = store.search(args.query, limit=args.limit, category=args.category)

    if not results:
        print("No matches.")
        return 0

    print_entries_grouped_by_day(results)
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pyrss", description="SQLite-backed RSS CLI")
    p.add_argument("--db", default=os.path.expanduser("~/.local/share/pyrss/rss.sqlite3"), help="Path to sqlite DB")

    sub = p.add_subparsers(dest="cmd", required=True)

    # add URL [CATEGORY]
    p_add = sub.add_parser("add", help="Register a new feed URL")
    p_add.add_argument("url")
    p_add.add_argument("category", nargs="?", default="default", help="Category name (default: 'default')")
    p_add.set_defaults(func=cmd_add)

    p_list = sub.add_parser("list", help="List registered feeds")
    p_list.set_defaults(func=cmd_list)

    p_delete = sub.add_parser("delete", help="Delete a feed (and its entries)")
    g_delete = p_delete.add_mutually_exclusive_group(required=True)
    g_delete.add_argument("--id", type=int, help="Feed id to delete")
    g_delete.add_argument("--url", help="Feed URL to delete")
    p_delete.set_defaults(func=cmd_delete)

    p_fetch = sub.add_parser("fetch", help="Fetch updates from all feeds and store in DB")
    p_fetch.set_defaults(func=cmd_fetch)

    # updates: either --date or --last N (default 1)
    p_updates = sub.add_parser("updates", help="Show updates for a day or from last N days (UTC)")
    g = p_updates.add_mutually_exclusive_group(required=False)
    g.add_argument("--date", help="YYYY-MM-DD (UTC)")
    g.add_argument("--last", type=int, default=1, help="Last N days inclusive (UTC). Default: 1")
    p_updates.add_argument("--fetch-first", action="store_true", help="Fetch before querying")
    p_updates.add_argument("--by-fetched", action="store_true", help="Filter by fetched_at instead of published_at")
    p_updates.set_defaults(func=cmd_updates)

    p_search = sub.add_parser("search", help="Full-text search (FTS5 when available)")
    p_search.add_argument("query", help='FTS query string, e.g. "python asyncio" or "kubernetes"')
    p_search.add_argument("--limit", type=int, default=50, help="Max results (default: 50, max: 500)")
    p_search.add_argument("--category", default=None, help="Filter by feed category")
    p_search.add_argument("--fetch-first", action="store_true", help="Fetch before searching")
    p_search.set_defaults(func=cmd_search)

    return p

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    raise SystemExit(main())
