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
