"""Fetch OpenAlex metadata for the IDCite seed papers.

IDCite v4 replaces every Scopus-derived seed-paper attribute with OpenAlex
(CC0) metadata. This script retrieves, for each seed DOI:

    1. The OpenAlex work record (title, dates, venue, authorships, citation
       count, bibliographic details, open-access status, funders), queried in
       batches of 50 DOIs and, for DOIs that cannot be batched or are not found
       in a batch, one by one.
    2. The OpenAlex institution records referenced by any authorship, which
       provide the city and country names of each affiliation.

Records are cached as JSON lines (``works.jsonl``, ``institutions.jsonl``) so
the script can be interrupted and resumed.

Usage
-----
    python fetch_openalex_seed.py \
        --seed-parquet /path/to/v3/seed_cited_papers.parquet \
        --cache-dir /path/to/openalex_cache
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

API = "https://api.openalex.org"
BATCH_SIZE = 50
SLEEP_SEC = 0.15
WORK_SELECT = (
    "id,doi,title,publication_date,publication_year,type,cited_by_count,"
    "primary_location,authorships,biblio,open_access,ids,funders,awards"
)
INSTITUTION_SELECT = "id,display_name,ror,country_code,geo,type"

SESSION = requests.Session()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def norm_doi(doi: Optional[str]) -> str:
    d = (doi or "").strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d


def short_id(x: Optional[str]) -> Optional[str]:
    return x.rsplit("/", 1)[-1] if isinstance(x, str) else None


def get_json(url: str, params: dict, allow_404: bool = False) -> Optional[dict]:
    """GET with retries on rate limiting and transient server errors."""
    for attempt in range(6):
        r = SESSION.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404 and allow_404:
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            print(f"  HTTP {r.status_code}, retry {attempt + 1}")
            time.sleep(5 * (attempt + 1))
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    raise RuntimeError(f"too many retries: {url}")


def read_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def write_jsonl(path: Path, records: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Stage 1: work records
# ---------------------------------------------------------------------------
def fetch_works_batched(dois: List[str], works_path: Path) -> None:
    """Query DOIs in batches; DOIs not returned are cached as empty records."""
    with works_path.open("a", encoding="utf-8") as out:
        for i in range(0, len(dois), BATCH_SIZE):
            batch = dois[i:i + BATCH_SIZE]
            j = get_json(
                f"{API}/works",
                {"filter": "doi:" + "|".join(batch), "select": WORK_SELECT, "per-page": 100},
            )
            found: Dict[str, dict] = {}
            for w in j["results"]:
                found.setdefault(norm_doi(w.get("doi")), w)
            for d in batch:
                rec = found.get(d) or {}
                rec["_query_doi"] = d
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            if (i // BATCH_SIZE) % 20 == 0:
                print(f"  works {i + len(batch)}/{len(dois)}")
            time.sleep(SLEEP_SEC)


def fetch_work_single(doi: str) -> Optional[dict]:
    """Look up one DOI, also trying its URL-decoded form (e.g. ``%23`` -> ``#``)."""
    for cand in dict.fromkeys([urllib.parse.unquote(doi), doi]):
        url = f"{API}/works/" + urllib.parse.quote("https://doi.org/" + cand, safe="")
        w = get_json(url, {"select": WORK_SELECT}, allow_404=True)
        time.sleep(SLEEP_SEC)
        if w:
            return w
    return None


def retry_unmatched(works_path: Path) -> None:
    records = read_jsonl(works_path)
    fixed = 0
    for rec in records:
        if rec.get("id"):
            continue
        w = fetch_work_single(rec["_query_doi"])
        if w:
            w["_query_doi"] = rec["_query_doi"]
            rec.clear()
            rec.update(w)
            fixed += 1
    write_jsonl(works_path, records)
    missing = sum(1 for r in records if not r.get("id"))
    print(f"  recovered by single lookup: {fixed} | not in OpenAlex: {missing}")


# ---------------------------------------------------------------------------
# Stage 2: institution records
# ---------------------------------------------------------------------------
def fetch_institutions(works_path: Path, inst_path: Path) -> None:
    needed = set()
    for w in read_jsonl(works_path):
        for a in w.get("authorships") or []:
            for inst in a.get("institutions") or []:
                if inst.get("id"):
                    needed.add(short_id(inst["id"]))
    have = {short_id(r["id"]) for r in read_jsonl(inst_path)}
    todo = sorted(needed - have)
    print(f"institutions: {len(needed)} referenced, {len(have)} cached, {len(todo)} to fetch")

    with inst_path.open("a", encoding="utf-8") as out:
        for i in range(0, len(todo), BATCH_SIZE):
            batch = todo[i:i + BATCH_SIZE]
            j = get_json(
                f"{API}/institutions",
                {"filter": "openalex:" + "|".join(batch), "select": INSTITUTION_SELECT,
                 "per-page": 100},
            )
            for inst in j["results"]:
                out.write(json.dumps(inst, ensure_ascii=False) + "\n")
            out.flush()
            time.sleep(SLEEP_SEC)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(seed_parquet: Path, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    works_path = cache_dir / "works.jsonl"
    inst_path = cache_dir / "institutions.jsonl"

    seed_dois = pd.read_parquet(seed_parquet, columns=["doi"])["doi"].dropna()
    dois = sorted({norm_doi(d) for d in seed_dois} - {""})
    cached = {r["_query_doi"] for r in read_jsonl(works_path)}
    todo = [d for d in dois if d not in cached]
    print(f"seed DOIs: {len(dois)} | cached: {len(cached)} | to fetch: {len(todo)}")

    # The OpenAlex filter syntax uses "," and "|" as separators.
    batchable = [d for d in todo if not any(c in d for c in ",|")]
    unbatchable = [d for d in todo if any(c in d for c in ",|")]

    print("\n[1/3] Fetching work records in batches ...")
    fetch_works_batched(batchable, works_path)
    with works_path.open("a", encoding="utf-8") as out:
        for d in unbatchable:
            rec = fetch_work_single(d) or {}
            rec["_query_doi"] = d
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n[2/3] Retrying unmatched DOIs one by one ...")
    retry_unmatched(works_path)

    print("\n[3/3] Fetching institution records ...")
    fetch_institutions(works_path, inst_path)
    print(f"\nDone. Cache written to: {cache_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OpenAlex work and institution records for the IDCite seed DOIs."
    )
    parser.add_argument("--seed-parquet", required=True, type=Path,
                        help="v3 seed_cited_papers.parquet (only the doi column is read).")
    parser.add_argument("--cache-dir", required=True, type=Path,
                        help="Directory for works.jsonl and institutions.jsonl.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(args.seed_parquet, args.cache_dir)


if __name__ == "__main__":
    main()
