#!/usr/bin/env python3
"""
Persist GitHub traffic (views + uniques) and repo counters (stars/forks/watchers)
into SQLite for multiple repositories.

Changes from original:
- config.json now contains a list of {owner, repo} objects under "repositories".
- SQLite tables now include 'owner' and 'repo' columns as part of the primary key.
- Loops over all repositories and stores metrics for each.
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
            owner TEXT NOT NULL,
            repo TEXT NOT NULL,
            date TEXT NOT NULL,
            views INTEGER NOT NULL,
            uniques INTEGER NOT NULL,
            PRIMARY KEY (owner, repo, date)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repo_counts_daily(
            owner TEXT NOT NULL,
            repo TEXT NOT NULL,
            date TEXT NOT NULL,
            stars INTEGER NOT NULL,
            forks INTEGER NOT NULL,
            watchers INTEGER NOT NULL,
            PRIMARY KEY (owner, repo, date)
        )
    """)
    conn.commit()
    return conn

def upsert_traffic_views(conn, owner, repo, rows):
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO traffic_views_daily(owner, repo, date, views, uniques)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(owner, repo, date) DO UPDATE SET
            views=excluded.views,
            uniques=excluded.uniques
    """, [(owner, repo, r["date"], r["views"], r["uniques"]) for r in rows])
    conn.commit()

def upsert_repo_counts(conn, owner, repo, snapshot):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO repo_counts_daily(owner, repo, date, stars, forks, watchers)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(owner, repo, date) DO UPDATE SET
            stars=excluded.stars,
            forks=excluded.forks,
            watchers=excluded.watchers
    """, (owner, repo, snapshot["date"], snapshot["stars"], snapshot["forks"], snapshot["watchers"]))
    conn.commit()

def fetch_traffic_views(owner, repo, token):
    payload = gh_get(f"/repos/{owner}/{repo}/traffic/views", token)
    rows = []
    for v in payload.get("views", []):
        d = v["timestamp"][:10]
        rows.append({"date": d, "views": int(v.get("count", 0)), "uniques": int(v.get("uniques", 0))})
    return rows

def fetch_repo_counts(owner, repo, token):
    repo_json = gh_get(f"/repos/{owner}/{repo}", token)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "date": now,
        "stars": int(repo_json.get("stargazers_count", 0)),
        "forks": int(repo_json.get("forks_count", 0)),
        "watchers": int(repo_json.get("subscribers_count", 0)),
    }

def print_rollups(conn):
    cur = conn.cursor()

    # Monthly roll-up per repo
    cur.execute("""
        SELECT owner, repo, substr(date, 1, 7) AS ym,
               SUM(views) AS views,
               SUM(uniques) AS sum_daily_uniques
        FROM traffic_views_daily
        GROUP BY owner, repo, ym
        ORDER BY owner, repo, ym
    """)
    rows = cur.fetchall()
    print("\n=== Historical monthly (per repo) ===")
    for owner, repo, ym, v, u in rows:
        print(f"{owner}/{repo} - {ym}: views={v:,} | daily-uniques-sum={u:,}")

    # Latest counts snapshot
    cur.execute("""
        SELECT owner, repo, date, stars, forks, watchers
        FROM repo_counts_daily
        WHERE (owner, repo, date) IN (
            SELECT owner, repo, MAX(date)
            FROM repo_counts_daily
            GROUP BY owner, repo
        )
        ORDER BY owner, repo
    """)
    latest = cur.fetchall()
    print("\n=== Latest counts snapshots ===")
    for owner, repo, d, stars, forks, watchers in latest:
        print(f"{owner}/{repo} [{d}]: ⭐ {stars:,}  | 🍴 {forks:,}  | 👀 {watchers:,}")

    print(f"\nSQLite DB: {DB_PATH.resolve()}")

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def main():
    config = load_config()
    token = config.get("github_token")
    repos = config.get("repositories", [])

    if not token:
        raise SystemExit("Error: github_token missing in config.json")
    if not repos:
        raise SystemExit("Error: repositories list is empty in config.json")

    conn = ensure_db()

    for repo_cfg in repos:
        owner = repo_cfg.get("owner")
        repo = repo_cfg.get("repo")
        if not owner or not repo:
            print(f"Skipping invalid repo entry: {repo_cfg}")
            continue

        print(f"Processing {owner}/{repo}...")

        # 1) Traffic
        traffic_rows = fetch_traffic_views(owner, repo, token)
        if traffic_rows:
            upsert_traffic_views(conn, owner, repo, traffic_rows)

        # 2) Repo counts snapshot
        snapshot = fetch_repo_counts(owner, repo, token)
        upsert_repo_counts(conn, owner, repo, snapshot)

    # 3) Print rollups
    print_rollups(conn)

if __name__ == "__main__":
    main()
