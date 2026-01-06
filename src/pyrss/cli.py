from __future__ import annotations

import argparse
import os
from datetime import date
from typing import List, Optional

from .store import Entry, RSSStore, parse_date_like


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
