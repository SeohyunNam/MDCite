"""Build the IDCite v4 release without Scopus-derived values.

IDCite v4 keeps the v3 citation events, citing papers, and citation intents
(Semantic Scholar contexts + SynIntent annotations) unchanged, and replaces
every Scopus-derived seed-paper attribute with OpenAlex (CC0) metadata
retrieved by ``fetch_openalex_seed.py``. The normalized entity tables and the
supplementary knowledge graph are then rebuilt with the same logic as
``../4_IDCite_Construction/ontology.py``.

Stages:

    1. Seed papers: v3 identifiers and sampling labels + OpenAlex metadata.
    2. Copy the unchanged Semantic Scholar-based tables.
    3. Rebuild ``citation_events_enriched`` with the OpenAlex seed lookup.
    4. Rebuild the normalized entity lookup tables.
    5. Rebuild the knowledge graph (``kg_nodes``, ``kg_edges``).

Usage
-----
    python build_idcite_v4.py \
        --v3-dir /path/to/citationhub_v1_ontology_ready \
        --cache-dir /path/to/openalex_cache \
        --out-dir /path/to/IDCite_v4
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "4_IDCite_Construction"))
from ontology import (  # noqa: E402  (same helpers as the v3 pipeline)
    attach_seed_metadata,
    build_edge_df,
    build_lookup_table,
    build_node_df,
    clean_str,
)

# Only non-Scopus attributes are kept from v3: identifiers derived from the DOI
# and the journal-stratified sampling labels (ESI field / WoS category / journal).
V3_SEED_KEEP = ["seed_paper_id", "doi", "group", "category", "journal_clarivate", "is_seed_top5pct"]

SEED_COLUMNS = [
    "seed_paper_id", "openalex_id", "doi", "title", "publication_date", "publication_year",
    "publication_name", "source_openalex_id", "issn_l", "creator", "creator_openalex_id",
    "author_count", "affilname", "affiliation_openalex_id", "affiliation_ror",
    "affiliation_city", "affiliation_country", "citedby_count", "type", "volume", "issue",
    "first_page", "last_page", "pmid", "is_oa", "oa_status", "funders", "group", "category",
    "journal_clarivate", "is_seed_top5pct", "metadata_source",
]

# citation_events_normalized only adds intent_id / field_id, which depend on the
# unchanged intents and fields tables, so these v3 files are reused as is.
UNCHANGED_TABLES = [
    "citation_events.parquet",
    "citation_events_normalized.parquet",
    "citing_papers.parquet",
    "intents.parquet",
    "fields.parquet",
]


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


def load_cache(cache_dir: Path):
    works: Dict[str, dict] = {}
    with (cache_dir / "works.jsonl").open(encoding="utf-8") as f:
        for line in f:
            w = json.loads(line)
            works[w["_query_doi"]] = w
    institutions: Dict[str, dict] = {}
    with (cache_dir / "institutions.jsonl").open(encoding="utf-8") as f:
        for line in f:
            inst = json.loads(line)
            institutions[short_id(inst["id"])] = inst
    return works, institutions


# ---------------------------------------------------------------------------
# Stage 1: seed papers from OpenAlex
# ---------------------------------------------------------------------------
def seed_row_from_openalex(w: dict, institutions: Dict[str, dict]) -> dict:
    if not w.get("id"):
        return {"metadata_source": None}

    loc = w.get("primary_location") or {}
    src = loc.get("source") or {}
    auths = w.get("authorships") or []
    first = next((a for a in auths if a.get("author_position") == "first"),
                 auths[0] if auths else {})
    first_author = (first or {}).get("author") or {}
    # Mirrors the v3 semantics: first affiliation listed on the paper.
    first_inst = next(
        (i for a in auths for i in (a.get("institutions") or []) if i.get("id")), None
    )
    inst = institutions.get(short_id(first_inst["id"])) if first_inst else None
    geo = (inst or {}).get("geo") or {}
    biblio = w.get("biblio") or {}
    oa = w.get("open_access") or {}
    funders = [f.get("display_name") for f in (w.get("funders") or []) if f.get("display_name")]

    return {
        "openalex_id": short_id(w.get("id")),
        "title": w.get("title"),
        "publication_date": w.get("publication_date"),
        "publication_year": w.get("publication_year"),
        "publication_name": src.get("display_name"),
        "source_openalex_id": short_id(src.get("id")),
        "issn_l": src.get("issn_l"),
        "creator": first_author.get("display_name"),
        "creator_openalex_id": short_id(first_author.get("id")),
        "author_count": len(auths) or None,
        "affilname": (first_inst or {}).get("display_name"),
        "affiliation_openalex_id": short_id((first_inst or {}).get("id")),
        "affiliation_ror": (first_inst or {}).get("ror"),
        "affiliation_city": geo.get("city"),
        "affiliation_country": geo.get("country"),
        "citedby_count": w.get("cited_by_count"),
        "type": w.get("type"),
        "volume": biblio.get("volume"),
        "issue": biblio.get("issue"),
        "first_page": biblio.get("first_page"),
        "last_page": biblio.get("last_page"),
        "pmid": short_id((w.get("ids") or {}).get("pmid")),
        "is_oa": oa.get("is_oa"),
        "oa_status": oa.get("oa_status"),
        "funders": "; ".join(funders) or None,
        "metadata_source": "OpenAlex",
    }


def build_seed_table(v3_dir: Path, works: Dict[str, dict],
                     institutions: Dict[str, dict]) -> pd.DataFrame:
    seed_v3 = pd.read_parquet(v3_dir / "seed_cited_papers.parquet", columns=V3_SEED_KEEP)
    rows = []
    for r in seed_v3.itertuples(index=False):
        row = {"seed_paper_id": r.seed_paper_id, "doi": r.doi}
        row.update(seed_row_from_openalex(works.get(norm_doi(r.doi), {}), institutions))
        row.update({"group": r.group, "category": r.category,
                    "journal_clarivate": r.journal_clarivate,
                    "is_seed_top5pct": r.is_seed_top5pct})
        rows.append(row)

    df = pd.DataFrame(rows).reindex(columns=SEED_COLUMNS)
    for col in ("citedby_count", "publication_year", "author_count"):
        df[col] = df[col].astype("Int64")
    return df


# ---------------------------------------------------------------------------
# Stage 3: enriched citation events
# ---------------------------------------------------------------------------
def build_enriched_events(events_path: Path, seed_df: pd.DataFrame, out_path: Path) -> None:
    """Append the OpenAlex seed lookup to every event, row-aligned via ``take``."""
    events = pq.read_table(events_path)
    cited_ids = events.column("cited_seed_paper_id").to_pandas()
    lookup = attach_seed_metadata(
        pd.DataFrame({"cited_seed_paper_id": pd.unique(cited_ids)}), seed_df
    )
    idx_of = pd.Series(range(len(lookup)), index=lookup["cited_seed_paper_id"])
    take = pa.array(idx_of.reindex(cited_ids).to_numpy(), from_pandas=True, type=pa.int64())

    lookup_tbl = pa.Table.from_pandas(
        lookup.drop(columns=["cited_seed_paper_id"]), preserve_index=False
    )
    enriched = events
    for col in lookup_tbl.column_names:
        enriched = enriched.append_column(col, lookup_tbl.column(col).take(take))
    pq.write_table(enriched, out_path)
    print(f"citation_events_enriched: ({enriched.num_rows}, {enriched.num_columns})")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_pipeline(v3_dir: Path, cache_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    works, institutions = load_cache(cache_dir)

    # Stage 1 -- seed (cited) papers
    print("\n[1/5] Building seed papers from OpenAlex ...")
    seed_df = build_seed_table(v3_dir, works, institutions)
    seed_df.to_parquet(out_dir / "seed_cited_papers.parquet", index=False)
    print(f"seed_cited_papers: {seed_df.shape} | "
          f"matched in OpenAlex: {seed_df['openalex_id'].notna().sum()}")

    # Stage 2 -- unchanged tables
    print("\n[2/5] Copying unchanged tables ...")
    for name in UNCHANGED_TABLES:
        shutil.copy2(v3_dir / name, out_dir / name)

    # Stage 3 -- enriched events
    print("\n[3/5] Rebuilding citation_events_enriched ...")
    build_enriched_events(out_dir / "citation_events.parquet", seed_df,
                          out_dir / "citation_events_enriched.parquet")

    # Stage 4 -- normalized entity lookup tables (as in ontology.py stage 4)
    print("\n[4/5] Normalizing entities ...")
    citing_papers_df = pd.read_parquet(out_dir / "citing_papers.parquet")
    journals_df = build_lookup_table(
        pd.concat([seed_df["publication_name"], seed_df["journal_clarivate"],
                   citing_papers_df["venue"]], ignore_index=True),
        "journal", "journal_id", "journal_name",
    )
    authors_df = build_lookup_table(seed_df["creator"], "author", "author_id", "author_name")
    countries_df = build_lookup_table(
        seed_df["affiliation_country"], "country", "country_id", "country_name"
    )
    cities_df = build_lookup_table(seed_df["affiliation_city"], "city", "city_id", "city_name")
    affiliations_df = build_lookup_table(
        seed_df["affilname"], "affiliation", "affiliation_id", "affiliation_name"
    )
    fields_df = pd.read_parquet(out_dir / "fields.parquet")
    intents_df = pd.read_parquet(out_dir / "intents.parquet")

    journal_map = dict(zip(journals_df["journal_name"], journals_df["journal_id"]))
    author_map = dict(zip(authors_df["author_name"], authors_df["author_id"]))
    affiliation_map = dict(zip(affiliations_df["affiliation_name"], affiliations_df["affiliation_id"]))
    city_map = dict(zip(cities_df["city_name"], cities_df["city_id"]))
    country_map = dict(zip(countries_df["country_name"], countries_df["country_id"]))
    field_map = dict(zip(fields_df["field_name"], fields_df["field_id"]))

    seed_norm = seed_df.copy()
    seed_norm["journal_id"] = seed_norm["publication_name"].map(clean_str).map(journal_map)
    seed_norm["author_id"] = seed_norm["creator"].map(clean_str).map(author_map)
    seed_norm["affiliation_id"] = seed_norm["affilname"].map(clean_str).map(affiliation_map)
    seed_norm["city_id"] = seed_norm["affiliation_city"].map(clean_str).map(city_map)
    seed_norm["country_id"] = seed_norm["affiliation_country"].map(clean_str).map(country_map)
    seed_norm["field_id"] = seed_norm["group"].map(clean_str).map(field_map)

    citing_norm = citing_papers_df.copy()
    citing_norm["journal_id"] = citing_norm["venue"].map(clean_str).map(journal_map)

    affiliation_geo_df = (
        seed_df[["affilname", "affiliation_city", "affiliation_country"]]
        .rename(columns={"affilname": "affiliation_name", "affiliation_city": "city_name",
                         "affiliation_country": "country_name"})
        .copy()
    )
    for col in ("affiliation_name", "city_name", "country_name"):
        affiliation_geo_df[col] = affiliation_geo_df[col].map(clean_str)
    affiliation_geo_df = (
        affiliation_geo_df.dropna(subset=["affiliation_name"]).drop_duplicates().reset_index(drop=True)
    )
    affiliation_geo_df["affiliation_id"] = affiliation_geo_df["affiliation_name"].map(affiliation_map)
    affiliation_geo_df["city_id"] = affiliation_geo_df["city_name"].map(city_map)
    affiliation_geo_df["country_id"] = affiliation_geo_df["country_name"].map(country_map)

    for name, df in {
        "journals": journals_df, "authors": authors_df, "affiliations": affiliations_df,
        "cities": cities_df, "countries": countries_df,
        "seed_cited_papers_normalized": seed_norm, "citing_papers_normalized": citing_norm,
        "affiliation_geo": affiliation_geo_df,
    }.items():
        df.to_parquet(out_dir / f"{name}.parquet", index=False)
        print(f"{name}: {df.shape}")

    # Stage 5 -- knowledge graph (as in ontology.py stage 5)
    print("\n[5/5] Building knowledge graph ...")
    events_norm = pd.read_parquet(
        out_dir / "citation_events_normalized.parquet",
        columns=["citation_event_id", "citing_paper_id", "cited_seed_paper_id", "primary_intent",
                 "context_count", "intent_count", "is_influential", "intent_id"],
    )
    events_norm["event_label"] = (
        "CitationEvent | " + events_norm["primary_intent"].fillna("unknown").astype(str)
    )

    kg_nodes_df = pd.concat(
        [
            build_node_df(seed_norm, "seed_paper_id", "title", "seed_paper",
                          ["doi", "publication_name", "group", "citedby_count"]),
            build_node_df(citing_norm, "citing_paper_id", "title", "citing_paper",
                          ["doi", "venue", "year"]),
            build_node_df(events_norm, "citation_event_id", "event_label", "citation_event",
                          ["primary_intent", "context_count", "intent_count", "is_influential"]),
            build_node_df(journals_df, "journal_id", "journal_name", "journal"),
            build_node_df(authors_df, "author_id", "author_name", "author"),
            build_node_df(affiliations_df, "affiliation_id", "affiliation_name", "affiliation"),
            build_node_df(cities_df, "city_id", "city_name", "city"),
            build_node_df(countries_df, "country_id", "country_name", "country"),
            build_node_df(fields_df, "field_id", "field_name", "field", ["category_raw"]),
            build_node_df(intents_df, "intent_id", "intent_name", "intent"),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["node_id"]).reset_index(drop=True)
    kg_nodes_df["citedby_count"] = kg_nodes_df["citedby_count"].astype("float64")

    kg_edges_df = pd.concat(
        [
            build_edge_df(events_norm, "citation_event_id", "citing_paper_id", "HAS_CITING_PAPER"),
            build_edge_df(events_norm, "citation_event_id", "cited_seed_paper_id", "HAS_CITED_PAPER"),
            build_edge_df(events_norm, "citation_event_id", "intent_id", "HAS_PRIMARY_INTENT"),
            build_edge_df(seed_norm, "seed_paper_id", "journal_id", "PUBLISHED_IN"),
            build_edge_df(seed_norm, "seed_paper_id", "author_id", "HAS_AUTHOR"),
            build_edge_df(seed_norm, "seed_paper_id", "affiliation_id", "HAS_AFFILIATION"),
            build_edge_df(seed_norm, "seed_paper_id", "field_id", "BELONGS_TO_FIELD"),
            build_edge_df(affiliation_geo_df, "affiliation_id", "city_id", "LOCATED_IN_CITY"),
            build_edge_df(affiliation_geo_df, "affiliation_id", "country_id", "LOCATED_IN_COUNTRY"),
            build_edge_df(citing_norm, "citing_paper_id", "journal_id", "PUBLISHED_IN_VENUE"),
        ],
        ignore_index=True,
    ).drop_duplicates().reset_index(drop=True)

    kg_nodes_df.to_parquet(out_dir / "kg_nodes.parquet", index=False)
    kg_edges_df.to_parquet(out_dir / "kg_edges.parquet", index=False)
    print(f"kg_nodes_df: {kg_nodes_df.shape} | kg_edges_df: {kg_edges_df.shape}")
    print(f"\nDone. Outputs written to: {out_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build IDCite v4 by replacing Scopus-derived seed metadata with OpenAlex."
    )
    parser.add_argument("--v3-dir", required=True, type=Path,
                        help="Directory with the v3 Parquet tables.")
    parser.add_argument("--cache-dir", required=True, type=Path,
                        help="OpenAlex cache written by fetch_openalex_seed.py.")
    parser.add_argument("--out-dir", required=True, type=Path,
                        help="Output directory for the 17 v4 Parquet tables.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(args.v3_dir, args.cache_dir, args.out_dir)


if __name__ == "__main__":
    main()
