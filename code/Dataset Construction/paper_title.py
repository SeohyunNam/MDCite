import os
import re
import csv
import time
import json
from typing import Optional, List, Dict

import requests
import pandas as pd

S2_BASE = "https://api.semanticscholar.org/graph/v1"
OA_BASE = "https://api.openalex.org"
HEADERS = {"User-Agent": "MDCite-SIGIR-2026", "Accept": "application/json"}


def slug(text: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]+', '_', text or "").strip('_')[:120]


# ------------------ OpenAlex ------------------

def openalex_resolve_doi(title: str):
    r = requests.get(
        f"{OA_BASE}/works",
        params={"search": title, "per_page": 25, "select": "id,doi,title"},
        timeout=30,
    )
    if r.status_code != 200:
        return None, None

    items = r.json().get("results", [])
    if not items:
        return None, None

    best = items[0]
    doi = (best.get("doi") or "").replace("https://doi.org/", "") or None
    return doi, best.get("title")


def openalex_all_citers_by_doi(doi: str):
    r = requests.get(f"{OA_BASE}/works/https://doi.org/{doi}")
    if r.status_code != 200:
        return []

    cited_by_url = r.json().get("cited_by_api_url")
    if not cited_by_url:
        return []

    rows = []
    cursor = "*"

    while True:
        r = requests.get(cited_by_url, params={"per_page": 200, "cursor": cursor})
        if r.status_code != 200:
            break
        data = r.json()
        rows += data.get("results", [])
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    return rows


# ------------------ Semantic Scholar ------------------

def s2_get_paper_id(doi: str) -> Optional[str]:
    r = requests.get(
        f"{S2_BASE}/paper/DOI:{doi}",
        params={"fields": "paperId,title"},
        headers=HEADERS,
        timeout=30,
    )
    if r.status_code != 200:
        return None
    return r.json().get("paperId")


def s2_fetch_citation_contexts(paper_id: str, limit: int):
    max_limit = 9999 if limit <= 0 else limit
    rows = []
    offset = 0
    page_size = 100

    fields = ",".join([
        "contexts",
        "intents",
        "isInfluential",
        "citingPaper.title",
        "citingPaper.year",
        "citingPaper.venue",
        "citingPaper.externalIds",
        "citingPaper.paperId"
    ])

    while len(rows) < max_limit:
        r = requests.get(
            f"{S2_BASE}/paper/{paper_id}/citations",
            params={"fields": fields, "limit": page_size, "offset": offset},
            headers=HEADERS,
            timeout=30,
        )

        if r.status_code != 200:
            break

        data = r.json().get("data", [])
        if not data:
            break

        for item in data:
            cp = item.get("citingPaper") or {}
            ext = cp.get("externalIds") or {}

            rows.append({
                "paperId": cp.get("paperId"),
                "title": cp.get("title"),
                "year": cp.get("year"),
                "venue": cp.get("venue"),
                "doi": ext.get("DOI"),
                "intents": item.get("intents"),
                "contexts": item.get("contexts"),
                "isInfluential": item.get("isInfluential"),
            })

        offset += page_size
        time.sleep(0.2)

        if len(rows) >= max_limit:
            break

    return rows[:max_limit]


# ======================================================
# REQUIRED SIGNATURE (compatible with batch script)
# ======================================================

def process_one(
    title: Optional[str],
    doi: Optional[str],
    outdir: str,
    limit: int,
    fetch_openalex: bool,
    fetch_scopus: bool,
    scopus_year_range: Optional[str],
):
    """
    Main entry point called by batch_paper_title_multi.py
    """

    print("\n=== Processing ===")
    print("Title:", title)
    print("DOI:", doi)

    # Resolve DOI via OpenAlex if needed
    if not doi and title:
        doi, _ = openalex_resolve_doi(title)

    if not doi:
        print("❌ Could not resolve DOI.")
        return

    print("Resolved DOI:", doi)

    subdir = os.path.join(outdir, slug(doi))
    os.makedirs(subdir, exist_ok=True)

    # 1️ Optional OpenAlex citing list
    if fetch_openalex:
        oa_rows = openalex_all_citers_by_doi(doi)
        if oa_rows:
            pd.DataFrame(oa_rows).to_csv(
                os.path.join(subdir, "openalex_citing_all.csv"),
                index=False,
                encoding="utf-8",
            )
            print(f"[OpenAlex] saved {len(oa_rows)} citing papers")

    # 2️ Semantic Scholar citation contexts
    paper_id = s2_get_paper_id(doi)
    if not paper_id:
        print("❌ S2 paperId not found.")
        return

    rows = s2_fetch_citation_contexts(paper_id, limit)

    with open(os.path.join(subdir, "citing_contexts.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(rows)} citation contexts.")

    # 3️ Scopus full citers (optional stub — no hard dependency)
    if fetch_scopus:
        print("[Info] Scopus full citing retrieval requires separate implementation and entitlement.")