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
import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import importlib


# -----------------------------
# Utilities
# -----------------------------


def _load_feedparser() -> Any:
    try:
        return importlib.import_module("feedparser")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "feedparser is required for fetching feeds. Install it with: pip install feedparser"
        ) from exc

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


@dataclass(frozen=True)
class FeedFilter:
    id: int
    feed_id: int
    name: str
    include_keywords: List[str]
    exclude_keywords: List[str]
    match_fields: List[str]
    is_case_sensitive: bool
    is_enabled: bool


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

CREATE TABLE IF NOT EXISTS feed_filters (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  feed_id           INTEGER NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  include_keywords  TEXT NOT NULL DEFAULT '[]',
  exclude_keywords  TEXT NOT NULL DEFAULT '[]',
  match_fields      TEXT NOT NULL DEFAULT '["title","summary","content"]',
  is_case_sensitive INTEGER NOT NULL DEFAULT 0,
  is_enabled        INTEGER NOT NULL DEFAULT 1,
  created_at        TEXT NOT NULL,

  UNIQUE(feed_id, name)
);

CREATE INDEX IF NOT EXISTS idx_feed_filters_feed_id ON feed_filters(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_filters_enabled ON feed_filters(is_enabled);
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

            filter_cols = {r["name"] for r in conn.execute("PRAGMA table_info(feed_filters)").fetchall()}
            if filter_cols:
                if "is_case_sensitive" not in filter_cols:
                    conn.execute("ALTER TABLE feed_filters ADD COLUMN is_case_sensitive INTEGER NOT NULL DEFAULT 0")
                if "is_enabled" not in filter_cols:
                    conn.execute("ALTER TABLE feed_filters ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1")

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

        d = _load_feedparser().parse(url, etag=etag)

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

    def get_last_entries_for_feed(
        self,
        feed_id: int,
        limit: int = 50,
        use_published_at: bool = True,
    ) -> List[Entry]:
        limit = max(1, min(int(limit), 500))
        col = "published_at" if use_published_at else "fetched_at"
        order = f"COALESCE({col}, fetched_at) DESC"

        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, feed_id, guid, title, link, author, published_at, summary, content, fetched_at
                FROM entries
                WHERE feed_id = ?
                ORDER BY {order}
                LIMIT ?
                """,
                (int(feed_id), limit),
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

    # ---- Feed filters ----

    @staticmethod
    def _parse_keywords(value: str) -> List[str]:
        raw = (value or "").strip()
        if not raw:
            return []

        if raw.startswith("["):
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise ValueError("Keywords JSON must be an array.")
            return [str(v).strip() for v in parsed if str(v).strip()]

        return [part.strip() for part in raw.split(",") if part.strip()]

    @staticmethod
    def _parse_match_fields(value: str) -> List[str]:
        default = ["title", "summary", "content"]
        raw = (value or "").strip()
        fields = default if not raw else RSSStore._parse_keywords(raw)
        valid = {"title", "summary", "content"}
        normalized = [f.lower() for f in fields]
        invalid = [f for f in normalized if f not in valid]
        if invalid:
            raise ValueError(f"Invalid match_fields: {', '.join(sorted(set(invalid)))}")
        if not normalized:
            raise ValueError("match_fields cannot be empty.")
        return list(dict.fromkeys(normalized))

    @staticmethod
    def _filter_from_row(row: sqlite3.Row) -> FeedFilter:
        return FeedFilter(
            id=int(row["id"]),
            feed_id=int(row["feed_id"]),
            name=str(row["name"]),
            include_keywords=json.loads(row["include_keywords"]),
            exclude_keywords=json.loads(row["exclude_keywords"]),
            match_fields=json.loads(row["match_fields"]),
            is_case_sensitive=bool(row["is_case_sensitive"]),
            is_enabled=bool(row["is_enabled"]),
        )

    def add_feed_filter(
        self,
        *,
        feed_id: int,
        name: str,
        include_keywords: str,
        exclude_keywords: str = "",
        match_fields: str = "title,summary,content",
        is_case_sensitive: bool = False,
    ) -> FeedFilter:
        filter_name = (name or "").strip()
        if not filter_name:
            raise ValueError("Filter name is required.")

        include = self._parse_keywords(include_keywords)
        if not include:
            raise ValueError("include_keywords must contain at least one keyword.")
        exclude = self._parse_keywords(exclude_keywords)
        fields = self._parse_match_fields(match_fields)

        with self._conn() as conn:
            exists = conn.execute("SELECT 1 FROM feeds WHERE id = ?", (int(feed_id),)).fetchone()
            if not exists:
                raise ValueError(f"Feed id {feed_id} does not exist.")

            conn.execute(
                """
                INSERT INTO feed_filters(
                    feed_id, name, include_keywords, exclude_keywords, match_fields, is_case_sensitive, is_enabled, created_at
                ) VALUES(?,?,?,?,?,?,1,?)
                """,
                (
                    int(feed_id),
                    filter_name,
                    json.dumps(include),
                    json.dumps(exclude),
                    json.dumps(fields),
                    int(bool(is_case_sensitive)),
                    utc_now_iso(),
                ),
            )
            row = conn.execute(
                """
                SELECT id, feed_id, name, include_keywords, exclude_keywords, match_fields, is_case_sensitive, is_enabled
                FROM feed_filters
                WHERE feed_id = ? AND name = ?
                """,
                (int(feed_id), filter_name),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to read newly added filter.")
            return self._filter_from_row(row)

    def list_feed_filters(self, feed_id: Optional[int] = None, only_enabled: bool = False) -> List[FeedFilter]:
        where: List[str] = []
        params: List[Any] = []

        if feed_id is not None:
            where.append("feed_id = ?")
            params.append(int(feed_id))
        if only_enabled:
            where.append("is_enabled = 1")

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, feed_id, name, include_keywords, exclude_keywords, match_fields, is_case_sensitive, is_enabled
                FROM feed_filters
                {where_sql}
                ORDER BY feed_id, id
                """,
                params,
            ).fetchall()
            return [self._filter_from_row(row) for row in rows]

    def deactivate_feed_filter(self, filter_id: int) -> int:
        with self._conn() as conn:
            cur = conn.execute("UPDATE feed_filters SET is_enabled = 0 WHERE id = ?", (int(filter_id),))
            return int(cur.rowcount)

    def remove_feed_filter(self, filter_id: int) -> int:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM feed_filters WHERE id = ?", (int(filter_id),))
            return int(cur.rowcount)

    def get_filtered_entries_for_feed(self, feed_id: int, limit: int = 50, use_published_at: bool = True) -> List[Entry]:
        entries = self.get_last_entries_for_feed(feed_id, limit=limit, use_published_at=use_published_at)
        filters = self.list_feed_filters(feed_id=feed_id, only_enabled=True)
        if not filters:
            return entries
        return [entry for entry in entries if self._entry_matches_any_filter(entry, filters)]

    @staticmethod
    def _entry_matches_any_filter(entry: Entry, filters: List[FeedFilter]) -> bool:
        for flt in filters:
            if RSSStore._entry_matches_filter(entry, flt):
                return True
        return False

    @staticmethod
    def _entry_matches_filter(entry: Entry, flt: FeedFilter) -> bool:
        chunks: List[str] = []
        for field in flt.match_fields:
            value = getattr(entry, field, None)
            if value:
                chunks.append(value)
        haystack = "\n".join(chunks)

        if not flt.is_case_sensitive:
            haystack = haystack.lower()
            include = [k.lower() for k in flt.include_keywords]
            exclude = [k.lower() for k in flt.exclude_keywords]
        else:
            include = flt.include_keywords
            exclude = flt.exclude_keywords

        includes_match = all(keyword in haystack for keyword in include)
        excludes_match = any(keyword in haystack for keyword in exclude)
        return includes_match and not excludes_match

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

def _sanitize_tsv_field(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.replace("\t", " ").replace("\n", " ").strip()

def _format_entry_tsv(entry: Entry) -> str:
    when = entry.published_at or entry.fetched_at or ""
    title = _sanitize_tsv_field(entry.title)
    link = _sanitize_tsv_field(entry.link)
    return f"{when}\t{title}\t{link}"

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

def cmd_recent(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)

    if args.fetch_first:
        store.fetch_all()

    use_published = not args.by_fetched
    entries = store.get_last_entries_for_feed(args.feed_id, limit=args.limit, use_published_at=use_published)

    if not entries:
        print("No entries found.")
        return 0

    for entry in entries:
        print(_format_entry_tsv(entry))
    return 0


def _format_filter_keywords(keywords: List[str]) -> str:
    return ",".join(keywords)


def cmd_filter_add(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    flt = store.add_feed_filter(
        feed_id=args.feed_id,
        name=args.name,
        include_keywords=args.include_keywords,
        exclude_keywords=args.exclude_keywords,
        match_fields=args.match_fields,
        is_case_sensitive=args.case_sensitive,
    )
    print(f"Added filter id={flt.id} feed_id={flt.feed_id} name={flt.name}")
    return 0


def cmd_filter_list(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    filters = store.list_feed_filters(feed_id=args.feed_id, only_enabled=args.enabled_only)
    if not filters:
        print("No filters found.")
        return 0

    for flt in filters:
        print(
            "\t".join(
                [
                    str(flt.id),
                    str(flt.feed_id),
                    flt.name,
                    _format_filter_keywords(flt.include_keywords),
                    _format_filter_keywords(flt.exclude_keywords),
                    _format_filter_keywords(flt.match_fields),
                    "1" if flt.is_case_sensitive else "0",
                    "1" if flt.is_enabled else "0",
                ]
            )
        )
    return 0


def cmd_filter_deactivate(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    changed = store.deactivate_feed_filter(args.filter_id)
    if changed == 0:
        print("No matching filter found.")
        return 1
    print(f"Deactivated {changed} filter(s).")
    return 0


def cmd_filter_remove(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)
    changed = store.remove_feed_filter(args.filter_id)
    if changed == 0:
        print("No matching filter found.")
        return 1
    print(f"Removed {changed} filter(s).")
    return 0


def cmd_recent_filtered(args: argparse.Namespace) -> int:
    store = RSSStore(args.db)

    if args.fetch_first:
        store.fetch_all()

    use_published = not args.by_fetched
    entries = store.get_filtered_entries_for_feed(args.feed_id, limit=args.limit, use_published_at=use_published)
    if not entries:
        print("No entries found.")
        return 0

    for entry in entries:
        print(_format_entry_tsv(entry))
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

    p_recent = sub.add_parser("recent", help="List recent entries for a feed (TSV)")
    p_recent.add_argument("feed_id", type=int, help="Feed ID to browse")
    p_recent.add_argument("--limit", type=int, default=50, help="Number of recent entries to load (default: 50)")
    p_recent.add_argument("--fetch-first", action="store_true", help="Fetch before listing")
    p_recent.add_argument("--by-fetched", action="store_true", help="Order by fetched_at instead of published_at")
    p_recent.set_defaults(func=cmd_recent)

    p_recent_filtered = sub.add_parser("recent-filtered", help="List recent entries for a feed after applying active feed filters (TSV)")
    p_recent_filtered.add_argument("feed_id", type=int, help="Feed ID to browse")
    p_recent_filtered.add_argument("--limit", type=int, default=50, help="Number of recent entries to load (default: 50)")
    p_recent_filtered.add_argument("--fetch-first", action="store_true", help="Fetch before listing")
    p_recent_filtered.add_argument("--by-fetched", action="store_true", help="Order by fetched_at instead of published_at")
    p_recent_filtered.set_defaults(func=cmd_recent_filtered)

    p_filter = sub.add_parser("filter", help="Manage saved feed filters")
    sub_filter = p_filter.add_subparsers(dest="filter_cmd", required=True)

    p_filter_add = sub_filter.add_parser("add", help="Add a saved filter for a feed")
    p_filter_add.add_argument("feed_id", type=int, help="Feed ID")
    p_filter_add.add_argument("name", help="Friendly filter name")
    p_filter_add.add_argument("include_keywords", help="Keywords to include (comma-separated or JSON array)")
    p_filter_add.add_argument("--exclude-keywords", default="", help="Keywords to exclude (comma-separated or JSON array)")
    p_filter_add.add_argument("--match-fields", default="title,summary,content", help="Fields to search in: any of title,summary,content")
    p_filter_add.add_argument("--case-sensitive", action="store_true", help="Case-sensitive keyword matching")
    p_filter_add.set_defaults(func=cmd_filter_add)

    p_filter_list = sub_filter.add_parser("list", help="List saved filters")
    p_filter_list.add_argument("--feed-id", type=int, default=None, help="Optional feed ID")
    p_filter_list.add_argument("--enabled-only", action="store_true", help="Show only enabled filters")
    p_filter_list.set_defaults(func=cmd_filter_list)

    p_filter_deactivate = sub_filter.add_parser("deactivate", help="Disable a filter by id")
    p_filter_deactivate.add_argument("filter_id", type=int)
    p_filter_deactivate.set_defaults(func=cmd_filter_deactivate)

    p_filter_remove = sub_filter.add_parser("remove", help="Remove a filter by id")
    p_filter_remove.add_argument("filter_id", type=int)
    p_filter_remove.set_defaults(func=cmd_filter_remove)

    return p

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    raise SystemExit(main())
