"""
sync_maps.py — Standalone map sync script for GitHub Actions.

Runs automatically when .mxl files are pushed to GitHub.
Parses changed maps and upserts them to Neon PostgreSQL.

Usage:
    python sync_maps.py                    # sync all changed files in current push
    python sync_maps.py --all              # force sync all .mxl files in repo
    python sync_maps.py --file path/to/map.mxl  # sync a single file

Environment variables required:
    PG_URI      — PostgreSQL connection string (stored as GitHub Secret)
    GITHUB_TOKEN — GitHub token (auto-provided by GitHub Actions)
    GITHUB_REPOSITORY — owner/repo (auto-provided by GitHub Actions)
    CHANGED_FILES — space-separated list of changed .mxl paths (set in workflow)
"""

import os
import re
import sys
import json
import base64
import argparse
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
PG_URI           = os.environ.get("PG_URI", "")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "")  # "owner/repo"
CHANGED_FILES    = os.environ.get("CHANGED_FILES", "")       # space-separated paths
MXL_EXT         = {".mxl"}

# ── DB setup ───────────────────────────────────────────────────────────────────
def get_conn():
    if not PG_URI:
        raise RuntimeError("PG_URI environment variable not set")
    conn = psycopg2.connect(PG_URI)
    conn.autocommit = False
    _init_schema(conn)
    return conn

def _init_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS repos (
                id          SERIAL PRIMARY KEY,
                repo_url    TEXT UNIQUE NOT NULL,
                owner       TEXT NOT NULL,
                repo        TEXT NOT NULL,
                gh_token    TEXT,
                last_synced TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS maps (
                id          SERIAL PRIMARY KEY,
                repo_id     INTEGER REFERENCES repos(id) ON DELETE CASCADE,
                filename    TEXT NOT NULL,
                path        TEXT NOT NULL,
                sha         TEXT NOT NULL,
                segments    JSONB NOT NULL DEFAULT '{}',
                elements    JSONB NOT NULL DEFAULT '{}',
                parsed_at   TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(repo_id, path)
            );
            CREATE INDEX IF NOT EXISTS idx_maps_sha  ON maps(sha);
            CREATE INDEX IF NOT EXISTS idx_maps_repo ON maps(repo_id);
        """)
    conn.commit()

# ── GitHub API ─────────────────────────────────────────────────────────────────
def gh_headers():
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def get_all_mxl_files(owner, repo):
    """Fetch all .mxl files from repo using Git Trees API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/HEAD?recursive=1"
    resp = requests.get(url, headers=gh_headers(), timeout=60)
    resp.raise_for_status()
    tree = resp.json().get("tree", [])
    return [
        item for item in tree
        if item["type"] == "blob"
        and os.path.splitext(item["path"])[1].lower() in MXL_EXT
    ]

def download_file(owner, repo, path, sha=""):
    """Download a single file by SHA using Git Blobs API."""
    if sha:
        url = f"https://api.github.com/repos/{owner}/{repo}/git/blobs/{sha}"
        resp = requests.get(url, headers=gh_headers(), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return base64.b64decode(data["content"].replace("\n", ""))
    # Fallback: Contents API
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    resp = requests.get(url, headers=gh_headers(), timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return base64.b64decode(data["content"].replace("\n", ""))

# ── MXL Parsers ────────────────────────────────────────────────────────────────
def parse_mxl_segments(raw_bytes):
    """Parse segment-level usage (M/C) from a Sterling .mxl file."""
    content = raw_bytes.decode("latin-1")
    segments = {}
    for block in re.findall(r'<Segment>(.*?)</Segment>', content, re.DOTALL):
        name_m = re.search(r'<Name>([^<]+)</Name>', block)
        if not name_m:
            continue
        seg_name = name_m.group(1).strip()
        if ':' in seg_name or seg_name.startswith('temp'):
            continue
        min_m = re.search(r'<Min>(\d+)</Min>', block)
        min_v = int(min_m.group(1)) if min_m else 0
        if seg_name not in segments:
            segments[seg_name] = 'M' if min_v >= 1 else 'C'
    return segments

def parse_map_elements_mxl(raw_bytes):
    """Parse element-level mandatory/conditional + conditional rules from .mxl."""
    content = raw_bytes.decode("latin-1")
    elements = {}
    for block in re.findall(r'<Segment>(.*?)</Segment>', content, re.DOTALL):
        name_m = re.search(r'<Name>([^<]+)</Name>', block)
        if not name_m:
            continue
        seg_name = name_m.group(1).strip()
        if ':' in seg_name or seg_name.startswith('temp'):
            continue

        seg_fields = []
        # Build field_id → ref map for SubjectElement resolution
        field_id_map = {}
        for fp in re.findall(r'<Field>(.*?)</Field>', block, re.DOTALL):
            fid = re.search(r'<ID>(\d+)</ID>', fp)
            fn  = re.search(r'<n>([^<]+)</n>', fp)
            if not fid or not fn:
                continue
            fn_val = fn.group(1).strip()
            if fn_val.startswith('temp'):
                continue
            b = fn_val.split(':')[0]
            try:    field_id_map[fid.group(1)] = str(int(b))
            except: field_id_map[fid.group(1)] = b.lstrip('0') or b

        for fblock in re.findall(r'<Field>(.*?)</Field>', block, re.DOTALL):
            fn_m = re.search(r'<Name>([^<]+)</Name>', fblock)
            fm_m = re.search(r'<Mandatory>(yes|no)</Mandatory>', fblock, re.IGNORECASE)
            if not fn_m or not fm_m:
                continue
            fname = fn_m.group(1).strip()
            if fname.startswith('temp'):
                continue
            base = fname.split(':')[0]
            try:    ref = str(int(base))
            except: ref = base.lstrip('0') or base

            # ConditionalRuleDef extraction
            rel_block_m = re.search(
                r'<ConditionalRuleDef>(.*?)</ConditionalRuleDef>', fblock, re.DOTALL)
            relation_code = None
            subject_refs  = []
            if rel_block_m:
                rb = rel_block_m.group(1)
                rc_m = re.search(r'<RelationCode>([^<]+)</RelationCode>', rb)
                relation_code = rc_m.group(1).strip().lower() if rc_m else None
                subject_fids = re.findall(r'<FieldID>(\d+)</FieldID>', rb)
                subject_refs = [
                    field_id_map[sid]
                    for sid in subject_fids
                    if sid in field_id_map
                ]

            seg_fields.append({
                "ref":           ref,
                "mandatory":     fm_m.group(1).lower() == 'yes',
                "relation_code": relation_code,
                "subject_refs":  subject_refs,
            })

        if seg_name not in elements:
            elements[seg_name] = seg_fields
        else:
            existing_refs = [f["ref"] for f in elements[seg_name]]
            for field in seg_fields:
                if field["ref"] not in existing_refs:
                    elements[seg_name].append(field)

    return elements

# ── DB operations ──────────────────────────────────────────────────────────────
def upsert_repo(conn, owner, repo):
    """Upsert repo row and return its id."""
    repo_url = f"https://github.com/{owner}/{repo}"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO repos (repo_url, owner, repo)
            VALUES (%s, %s, %s)
            ON CONFLICT (repo_url) DO UPDATE
                SET owner=EXCLUDED.owner, repo=EXCLUDED.repo
            RETURNING id
        """, (repo_url, owner, repo))
        row = cur.fetchone()
    conn.commit()
    return row["id"]

def get_existing_shas(conn, repo_id):
    """Return {path: sha} for all maps in this repo."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT path, sha FROM maps WHERE repo_id = %s", (repo_id,))
        return {row["path"]: row["sha"] for row in cur.fetchall()}

def upsert_map(conn, repo_id, filename, path, sha, segs, elems):
    """Insert or update a map row."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO maps
                (repo_id, filename, path, sha, segments, elements, parsed_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
            ON CONFLICT (repo_id, path) DO UPDATE
                SET sha       = EXCLUDED.sha,
                    segments  = EXCLUDED.segments,
                    elements  = EXCLUDED.elements,
                    parsed_at = NOW()
        """, (
            repo_id, filename, path, sha,
            json.dumps(segs),
            json.dumps(elems),
        ))
    conn.commit()

def delete_map(conn, repo_id, path):
    """Delete a map that was removed from GitHub."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM maps WHERE repo_id=%s AND path=%s", (repo_id, path))
    conn.commit()

def update_last_synced(conn, repo_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE repos SET last_synced=NOW() WHERE id=%s", (repo_id,))
    conn.commit()

# ── Main sync logic ────────────────────────────────────────────────────────────
def sync_files(owner, repo, files_to_sync, conn, repo_id, existing_shas):
    """
    Download, parse, and upsert a list of files.
    files_to_sync: list of {path, sha} dicts
    """
    added = updated = skipped = 0
    errors = []

    for item in files_to_sync:
        path = item["path"]
        sha  = item.get("sha", "")
        filename = os.path.basename(path)

        # Skip if SHA unchanged
        if existing_shas.get(path) == sha and sha:
            print(f"  [SKIP] {filename} (SHA unchanged)")
            skipped += 1
            continue

        try:
            print(f"  [DOWNLOAD] {filename} ...")
            raw  = download_file(owner, repo, path, sha=sha)

            print(f"  [PARSE]    {filename} ...")
            segs = parse_mxl_segments(raw)
            if not segs:
                raise ValueError("No segments parsed — file may be invalid")
            elems = parse_map_elements_mxl(raw)

            is_new = path not in existing_shas
            upsert_map(conn, repo_id, filename, path, sha, segs, elems)

            if is_new:
                print(f"  [ADDED]    {filename} ({len(segs)} segs)")
                added += 1
            else:
                print(f"  [UPDATED]  {filename} ({len(segs)} segs)")
                updated += 1

        except Exception as e:
            print(f"  [ERROR]    {filename}: {e}")
            errors.append({"path": path, "error": str(e)})

    return added, updated, skipped, errors


def main():
    parser = argparse.ArgumentParser(description="Sync .mxl maps to PostgreSQL")
    parser.add_argument("--all",  action="store_true",
                        help="Force sync all .mxl files in repo")
    parser.add_argument("--file", type=str, default="",
                        help="Sync a single specific file path")
    parser.add_argument("--delete", type=str, default="",
                        help="Delete a specific file path from DB")
    args = parser.parse_args()

    # Validate environment
    if not PG_URI:
        print("ERROR: PG_URI environment variable not set")
        sys.exit(1)
    if not GITHUB_REPOSITORY:
        print("ERROR: GITHUB_REPOSITORY environment variable not set")
        sys.exit(1)

    owner, repo = GITHUB_REPOSITORY.split("/", 1)
    print(f"\n{'='*60}")
    print(f"EDI 204 Map Sync — {owner}/{repo}")
    print(f"{'='*60}")

    # Connect to PostgreSQL
    print("\n[DB] Connecting to PostgreSQL...")
    conn = get_conn()
    print("[DB] Connected. Schema ready.")

    # Upsert repo
    repo_id = upsert_repo(conn, owner, repo)
    print(f"[DB] Repo ID: {repo_id}")

    existing_shas = get_existing_shas(conn, repo_id)
    print(f"[DB] Existing maps in DB: {len(existing_shas)}")

    # ── Handle --delete ────────────────────────────────────────────────────────
    if args.delete:
        path = args.delete
        print(f"\n[DELETE] Removing {path} from DB...")
        delete_map(conn, repo_id, path)
        print(f"[DELETE] Done.")
        return

    # ── Determine which files to sync ─────────────────────────────────────────
    if args.all:
        # Fetch all .mxl files from repo
        print("\n[GITHUB] Fetching all .mxl files from repo...")
        all_files = get_all_mxl_files(owner, repo)
        files_to_sync = all_files
        print(f"[GITHUB] Found {len(files_to_sync)} .mxl files")

    elif args.file:
        # Single file specified
        files_to_sync = [{"path": args.file, "sha": ""}]
        print(f"\n[SYNC] Single file mode: {args.file}")

    elif CHANGED_FILES:
        # Changed files from GitHub Actions environment variable
        changed_paths = [p.strip() for p in CHANGED_FILES.split() if p.strip()]
        mxl_paths = [
            p for p in changed_paths
            if os.path.splitext(p)[1].lower() in MXL_EXT
        ]
        print(f"\n[GITHUB] Changed .mxl files in this push: {len(mxl_paths)}")
        for p in mxl_paths:
            print(f"  - {p}")

        if not mxl_paths:
            print("[SKIP] No .mxl files changed in this push. Nothing to sync.")
            return

        # Fetch SHAs for changed files from GitHub
        print("\n[GITHUB] Fetching SHAs for changed files...")
        all_files = get_all_mxl_files(owner, repo)
        sha_map = {item["path"]: item["sha"] for item in all_files}
        files_to_sync = [
            {"path": p, "sha": sha_map.get(p, "")}
            for p in mxl_paths
            if p in sha_map  # file still exists (not deleted)
        ]

        # Handle deleted files
        deleted_paths = [p for p in mxl_paths if p not in sha_map]
        for dp in deleted_paths:
            print(f"  [DELETE] {dp} removed from repo → removing from DB")
            delete_map(conn, repo_id, dp)

    else:
        print("\n[INFO] No CHANGED_FILES set and --all not specified.")
        print("[INFO] Running full sync of all .mxl files in repo...")
        all_files = get_all_mxl_files(owner, repo)
        files_to_sync = all_files
        print(f"[GITHUB] Found {len(files_to_sync)} .mxl files")

    # ── Sync ──────────────────────────────────────────────────────────────────
    print(f"\n[SYNC] Processing {len(files_to_sync)} file(s)...")
    added, updated, skipped, errors = sync_files(
        owner, repo, files_to_sync, conn, repo_id, existing_shas)

    # Update last synced timestamp
    update_last_synced(conn, repo_id)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = added + updated + skipped
    print(f"\n{'='*60}")
    print(f"SYNC COMPLETE")
    print(f"{'='*60}")
    print(f"  Added   : {added}")
    print(f"  Updated : {updated}")
    print(f"  Skipped : {skipped} (SHA unchanged)")
    print(f"  Errors  : {len(errors)}")
    if errors:
        for e in errors:
            print(f"    ERROR: {e['path']} — {e['error']}")
    print(f"{'='*60}\n")

    conn.close()

    if errors:
        sys.exit(1)  # Fail the GitHub Action if any errors


if __name__ == "__main__":
    main()
