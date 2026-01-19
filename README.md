pyRSS — SQLite-backed RSS reader (CLI + Python API)
===================================================

rsscli is a small Python RSS reader that:
- Stores your registered feeds in SQLite
- Fetches new entries from all feeds (deduplicated)
- Lets you query entries from a given day
- Exposes a simple Python API so other scripts can: fetch -> query

It uses:
- sqlite3 (built-in)
- feedparser (external dependency)

------------------------------------------------------------
Install
------------------------------------------------------------

Requirements:
- Python 3.9+ recommended
- pip

Install dependency:

  pip install feedparser

(Optional) Make it executable:

  chmod +x rsscli.py

------------------------------------------------------------
Database
------------------------------------------------------------

By default the database file is:

  ~/.local/share/rsscli/rss.sqlite3

You can override with:

  python rsscli.py --db /path/to/rss.sqlite3 <command> ...

Tables:
- feeds: registered feed URLs + category + metadata
- entries: stored feed items, deduped by (feed_id, guid)

Notes:
- Entries are stored with timestamps in UTC (ISO8601).
- "published_at" comes from the feed when available.
- "fetched_at" is when rsscli saved it to the DB.

------------------------------------------------------------
CLI usage
------------------------------------------------------------

1) Add a feed (with optional category)

  python rsscli.py add https://hnrss.org/frontpage dev
  python rsscli.py add https://planetpython.org/rss20.xml
  # if category is omitted, it defaults to: "default"

2) List feeds

  python rsscli.py list

Output columns:
  <id> <category> <url> <title>

3) Fetch updates from all feeds

  python rsscli.py fetch

This will:
- Download feeds
- Insert new entries (skips already-seen entries)

4) Show updates for a day (UTC)

  python rsscli.py updates --date 2025-12-23

5) Fetch first, then show updates for a day

  python rsscli.py updates --date 2025-12-23 --fetch-first

6) If a feed doesn’t have reliable "published" timestamps,
   you can filter by the day entries were fetched:

  python rsscli.py updates --date 2025-12-23 --fetch-first --by-fetched

7) Browse the last 50 entries for a feed with fzf and open a link

  python rsscli.py fzf <feed_id>

You can change the number of entries or ordering:

  python rsscli.py fzf <feed_id> --limit 100
  python rsscli.py fzf <feed_id> --by-fetched


------------------------------------------------------------
Python API usage
------------------------------------------------------------

You can import RSSStore from rsscli.py and use it from another script.

Example:

  from datetime import date
  from rsscli import RSSStore

  store = RSSStore("/path/to/rss.sqlite3")

  # One-call behavior: fetch + then query
  updates = store.fetch_and_get_updates_for_day(date(2025, 12, 23))

  for e in updates:
      print(e.title, e.link)

Other useful methods:
- store.add_feed(url, category="default")
- store.list_feeds()
- store.fetch_all()
- store.get_updates_for_day(day)

------------------------------------------------------------
Design choices / notes
------------------------------------------------------------

Deduplication:
- Each entry is identified by a "guid" generated as:
  - entry.id / entry.guid (if present), else
  - entry.link (if present), else
  - hash(title + published)

Timezone:
- rsscli stores times in UTC for simpler querying and consistent behavior.

Schema migration:
- If you created the DB before categories were added, rsscli will attempt
  to ALTER the feeds table to add the "category" column automatically.

------------------------------------------------------------
Troubleshooting
------------------------------------------------------------

- "No feeds registered."
  Add at least one feed:
    python rsscli.py add https://example.com/feed.xml

- Some feeds show empty "published_at"
  That’s normal for certain feeds. Use:
    rsscli.py updates --by-fetched

- Feed fetch errors
  Run:
    python rsscli.py fetch
  and check the printed error list.

------------------------------------------------------------
License
------------------------------------------------------------

Choose a license (MIT/Apache-2.0/etc.) and put it in LICENSE.
