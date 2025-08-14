#!/usr/bin/env python3
"""
Persist GitHub traffic (views + uniques) and repo counters (stars/forks/watchers) into SQLite.

What you get
------------
Tables:
  - traffic_views_daily(date TEXT PRIMARY KEY, views INTEGER, uniques INTEGER)
  - repo_counts_daily(date TEXT PRIMARY KEY, stars INTEGER, forks INTEGER, watchers INTEGER)

Behaviors:
  - Fetches last 14 days of daily traffic: GET /repos/{owner}/{repo}/traffic/views
  - Snapshots today's counts (stars/forks/watchers) from:   GET /repos/{owner}/{repo}
  - Upserts into SQLite so the job is idempotent
  - Prints:
      * Month-to-date (views and sum of daily uniques; note this overcounts true monthly uniques)
      * Latest stars/forks/watchers snapshot

Auth
----
Set env var GITHUB_TOKEN with a PAT:
  - Public repos you own/collab: fine-grained PAT with Repository->Traffic: Read (or Classic token w/ no scopes usually works for public, but FG-Pat is cleaner).
  - Private repos: Classic PAT with 'repo'  OR  Fine-grained PAT with Repository access (select repo) and Permission 'Traffic: Read'. (Metadata Read is automatic.)
"""

import os
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API_ROOT = "https://api.github.com"
DB_PATH = Path("data/github_metrics.sqlite3")
# Load config.json from same directory as script
CONFIG_PATH = Path(__file__).parent / "config.json"

def gh_get(path: str, token: str, accept: str = "application/vnd.github+json"):
    url = f"{API_ROOT}{path}"
    req = Request(url)
    req.add_header("Accept", accept)
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise SystemExit(f"GitHub API error {e.code} for {path}: {body}")
    except URLError as e:
        raise SystemExit(f"Network error calling {path}: {e}")

def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_views_daily(
            date TEXT PRIMARY KEY,
            views INTEGER NOT NULL,
            uniques INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_counts_daily(
            date TEXT PRIMARY KEY,
            stars INTEGER NOT NULL,
            forks INTEGER NOT NULL,
            watchers INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn

def upsert_traffic_views(conn, rows):
    """
    rows: list of dicts with keys: date (YYYY-MM-DD), views, uniques
    """
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO traffic_views_daily(date, views, uniques)
        VALUES(?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            views=excluded.views,
            uniques=excluded.uniques
    """, [(r["date"], r["views"], r["uniques"]) for r in rows])
    conn.commit()

def upsert_repo_counts(conn, snapshot):
    """
    snapshot: dict with keys: date (YYYY-MM-DD), stars, forks, watchers
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO repo_counts_daily(date, stars, forks, watchers)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            stars=excluded.stars,
            forks=excluded.forks,
            watchers=excluded.watchers
    """, (snapshot["date"], snapshot["stars"], snapshot["forks"], snapshot["watchers"]))
    conn.commit()

def fetch_traffic_views(owner, repo, token):
    payload = gh_get(f"/repos/{owner}/{repo}/traffic/views", token)
    # payload: {"count": int, "uniques": int, "views": [{"timestamp":"YYYY-MM-DDTHH:MM:SSZ","count":X,"uniques":Y}, ...]}
    rows = []
    for v in payload.get("views", []):
        d = v["timestamp"][:10]  # YYYY-MM-DD
        rows.append({"date": d, "views": int(v.get("count", 0)), "uniques": int(v.get("uniques", 0))})
    return rows

def fetch_repo_counts(owner, repo, token):
    repo_json = gh_get(f"/repos/{owner}/{repo}", token)
    # stargazers_count = "Stars", forks_count = "Forks", subscribers_count = "Watchers" (people watching releases/notifications)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "date": now,
        "stars": int(repo_json.get("stargazers_count", 0)),
        "forks": int(repo_json.get("forks_count", 0)),
        "watchers": int(repo_json.get("subscribers_count", 0)),  # note: not stargazers_count
    }

def print_rollups(conn):
    cur = conn.cursor()

    # Monthly roll-up for views and sum of daily uniques (overcounts true monthly uniques)
    cur.execute("""
        SELECT substr(date, 1, 7) AS ym,
               SUM(views) AS views,
               SUM(uniques) AS sum_daily_uniques
        FROM traffic_views_daily
        GROUP BY ym
        ORDER BY ym
    """)
    rows = cur.fetchall()
    print("\n=== Historical monthly (views, sum of daily uniques) ===")
    for ym, v, u in rows:
        print(f"{ym}: views={v:,} | daily-uniques-sum={u:,}")

    # MTD convenience (based on UTC month)
    current_ym = datetime.now(timezone.utc).strftime("%Y-%m")
    cur.execute("""
        SELECT SUM(views), SUM(uniques)
        FROM traffic_views_daily
        WHERE substr(date,1,7)=?
    """, (current_ym,))
    m = cur.fetchone()
    if m and any(m):
        v, u = m
        print(f"\nMTD ({current_ym}): views={v:,} | daily-uniques-sum={u:,}")

    # Latest counts snapshot
    cur.execute("""
        SELECT date, stars, forks, watchers
        FROM repo_counts_daily
        ORDER BY date DESC
        LIMIT 1
    """)
    latest = cur.fetchone()
    if latest:
        d, stars, forks, watchers = latest
        print(f"\nLatest counts snapshot [{d}]: ⭐ {stars:,}  | 🍴 {forks:,}  | 👀 {watchers:,}")

    print(f"\nSQLite DB: {DB_PATH.resolve()}")

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def main():
    config = load_config()
    token = config.get("github_token")
    owner = config.get("owner")
    repo = config.get("repo")
    conn = ensure_db()

    # 1) Traffic (last 14 days)
    traffic_rows = fetch_traffic_views(owner, repo, token)
    if traffic_rows:
        upsert_traffic_views(conn, traffic_rows)

    # 2) Snapshot repo counts (stars/forks/watchers) for today
    snapshot = fetch_repo_counts(owner, repo, token)
    upsert_repo_counts(conn, snapshot)

    # 3) Print rollups
    print_rollups(conn)

if __name__ == "__main__":
    main()
