from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

import pyrss  # noqa: E402


def test_entry_guid_priority_order() -> None:
    entry = {"id": "abc-123", "guid": "ignored", "link": "https://example.com"}
    assert pyrss.entry_guid(entry) == "abc-123"

    entry = {"guid": "guid-456", "link": "https://example.com"}
    assert pyrss.entry_guid(entry) == "guid-456"

    entry = {"link": "https://example.com/item"}
    assert pyrss.entry_guid(entry) == "https://example.com/item"

    entry = {"title": "Example", "published": "2024-01-02"}
    expected_raw = "Example|2024-01-02".encode("utf-8")
    assert pyrss.entry_guid(entry) == hashlib.sha256(expected_raw).hexdigest()


def test_add_feed_updates_category(tmp_path: Path) -> None:
    db_path = tmp_path / "rss.sqlite3"
    store = pyrss.RSSStore(str(db_path))

    store.add_feed("https://example.com/rss", category="news")
    store.add_feed("https://example.com/rss", category="dev")

    feeds = store.list_feeds()
    assert len(feeds) == 1
    assert feeds[0].category == "dev"


def test_feed_filters_apply_and_deactivate(tmp_path: Path) -> None:
    db_path = tmp_path / "rss.sqlite3"
    store = pyrss.RSSStore(str(db_path))

    feed = store.add_feed("https://example.com/rss", category="news")

    with store._conn() as conn:
        conn.execute(
            """
            INSERT INTO entries (feed_id, guid, title, link, author, published_at, summary, content, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (feed.id, "g1", "AI startup raises seed", "https://e/1", None, "2025-01-01T00:00:00+00:00", "", "", "2025-01-01T00:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO entries (feed_id, guid, title, link, author, published_at, summary, content, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (feed.id, "g2", "Sports daily", "https://e/2", None, "2025-01-02T00:00:00+00:00", "", "", "2025-01-02T00:00:00+00:00"),
        )

    flt = store.add_feed_filter(
        feed_id=feed.id,
        name="AI + startup",
        include_keywords="ai,startup",
        exclude_keywords="seed",
        match_fields="title",
        is_case_sensitive=False,
    )

    filtered = store.get_filtered_entries_for_feed(feed.id, limit=10)
    assert filtered == []

    store.deactivate_feed_filter(flt.id)
    filtered_after = store.get_filtered_entries_for_feed(feed.id, limit=10)
    assert len(filtered_after) == 2


def test_parse_keywords_json_array(tmp_path: Path) -> None:
    db_path = tmp_path / "rss.sqlite3"
    store = pyrss.RSSStore(str(db_path))

    feed = store.add_feed("https://example.com/another", category="dev")
    flt = store.add_feed_filter(
        feed_id=feed.id,
        name="json",
        include_keywords='["python", "asyncio"]',
        exclude_keywords='[]',
        match_fields='["title", "summary"]',
    )

    assert flt.include_keywords == ["python", "asyncio"]
    assert flt.match_fields == ["title", "summary"]
