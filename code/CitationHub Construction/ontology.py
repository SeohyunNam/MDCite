"""CitationHub ontology-ready knowledge graph construction pipeline.

This script builds the ontology-ready CitationHub resource from the two raw
inputs collected during dataset construction:

    1. Journal-stratified top-5%% seed (cited) papers exported from Scopus,
       provided as a ZIP archive of ``*_full.json`` files.
    2. Citing-paper citation contexts and citation intents retrieved from the
       Semantic Scholar Graph API, provided as a ZIP archive of
       ``citing_contexts.json`` files organized by ESI field folder and by a
       per-seed-paper DOI bundle directory.

It produces the released Parquet tables described in the CitationHub
Scientific Data paper (Methods 2.4-2.9):

    seed_cited_papers.parquet           seed (cited) paper metadata
    citation_events.parquet             citation-event records
    citing_papers.parquet               citing-paper metadata
    citation_events_enriched.parquet    events joined with seed metadata
    journals/authors/affiliations/
      cities/countries/fields/
      intents.parquet                   normalized entity lookup tables
    seed_cited_papers_normalized.parquet
    citing_papers_normalized.parquet
    citation_events_normalized.parquet  id-normalized core tables
    affiliation_geo.parquet             affiliation -> city / country mapping
    kg_nodes.parquet                    ontology-ready knowledge-graph nodes
    kg_edges.parquet                    ontology-ready knowledge-graph edges

Usage
-----
    python ontology.py --base-dir /path/to/wos_data

Expected input layout under ``--base-dir``::

    <base-dir>/Top 5%% cited papers per journal dataset.zip
    <base-dir>/data_1242025_result_revised.zip

Outputs are written to ``<base-dir>/citationhub_v1_ontology_ready/``.
"""

from __future__ import annotations

import argparse
import json
import math
import zipfile
from pathlib import Path
from typing import Any, Iterable, List, Optional

import pandas as pd

CHUNK_SIZE = 100
OUTPUT_SUBDIR = "citationhub_v1_ontology_ready"
DEFAULT_CITED_ZIP = "Top 5% cited papers per journal dataset.zip"
DEFAULT_CITING_ZIP = "data_1242025_result_revised.zip"


# ---------------------------------------------------------------------------
# Value-normalization helpers
# ---------------------------------------------------------------------------
def is_missing(x: Any) -> bool:
    if x is None:
        return True
    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return False


def first_value(x: Any) -> Any:
    """Return the first meaningful value from a list/tuple/delimited string."""
    if is_missing(x):
        return None
    if isinstance(x, (list, tuple)):
        return first_value(x[0]) if len(x) else None
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        for sep in (";", "|", "///"):
            if sep in s:
                return s.split(sep)[0].strip()
        return s
    return x


def safe_int(x: Any) -> Optional[int]:
    if is_missing(x):
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def normalize_text(x: Any) -> Optional[str]:
    if is_missing(x):
        return None
    s = str(x).strip()
    return s or None


# ``clean_str`` is kept as a separate name because it is used in the
# normalization stage; behaviour is identical to ``normalize_text``.
clean_str = normalize_text


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------
def make_seed_paper_id(doi: Optional[str], eid: Optional[str]) -> str:
    if doi:
        return f"seed:{doi.lower()}"
    if eid:
        return f"seed_eid:{eid}"
    return "seed:unknown"


def make_citing_paper_id(
    doi: Optional[str], paper_id: Optional[str], title: Optional[str]
) -> str:
    if doi:
        return f"citing:{doi.lower()}"
    if paper_id:
        return f"citing_pid:{paper_id}"
    if title:
        return f"citing_title:{title[:100].lower()}"
    return "citing:unknown"


def parse_bundle_info(path: str) -> dict:
    """Extract the field folder and seed-paper DOI bundle from an inner path.

    Example input::

        .../output_Agricultural_Sciences/10.1016_j.aninu.2015.06.001/citing_contexts.json
    """
    parts = path.strip("/").split("/")
    out = {"raw_path": path, "field_folder": None, "bundle_id": None}
    if len(parts) >= 3:
        out["field_folder"] = parts[-3]
        out["bundle_id"] = parts[-2]
    return out


def bundle_id_to_doi(bundle_id: Optional[str]) -> Optional[str]:
    # Bundle directory names are DOIs whose slashes were replaced by underscores.
    if not bundle_id:
        return None
    return bundle_id.replace("_", "/")


# ---------------------------------------------------------------------------
# ZIP I/O helpers
# ---------------------------------------------------------------------------
def load_json_from_zip(zip_path: Path, inner_path: str):
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(inner_path) as f:
            return json.load(f)


def list_zip_files(zip_path: Path) -> List[str]:
    with zipfile.ZipFile(zip_path, "r") as z:
        return z.namelist()


# ---------------------------------------------------------------------------
# Stage 1: seed (cited) paper extraction
# ---------------------------------------------------------------------------
def load_seed_cited_papers(cited_zip: Path) -> pd.DataFrame:
    """Read every ``*_full.json`` seed-paper record into a normalized table."""
    rows = []
    json_files = [p for p in list_zip_files(cited_zip) if p.endswith("_full.json")]

    for jf in json_files:
        data = load_json_from_zip(cited_zip, jf)
        if not isinstance(data, list):
            continue

        for item in data:
            if not isinstance(item, dict):
                continue

            doi = normalize_text(item.get("doi"))
            eid = normalize_text(item.get("eid"))

            rows.append(
                {
                    "seed_paper_id": make_seed_paper_id(doi, eid),
                    "eid": eid,
                    "doi": doi,
                    "title": normalize_text(item.get("title")),
                    "cover_date": normalize_text(item.get("coverDate")),
                    "cover_display_date": normalize_text(item.get("coverDisplayDate")),
                    "publication_name": first_value(item.get("publicationName")),
                    "creator": first_value(item.get("creator")),
                    "citedby_count": safe_int(item.get("citedby_count")),
                    "subtype": normalize_text(item.get("subtype")),
                    "subtype_description": normalize_text(item.get("subtypeDescription")),
                    "pii": normalize_text(item.get("pii")),
                    "pubmed_id": normalize_text(item.get("pubmed_id")),
                    "afid": first_value(item.get("afid")),
                    "affilname": first_value(item.get("affilname")),
                    "affiliation_city": first_value(item.get("affiliation_city")),
                    "affiliation_country": first_value(item.get("affiliation_country")),
                    "author_count": safe_int(item.get("author_count")),
                    "author_names": first_value(item.get("author_names")),
                    "author_ids": first_value(item.get("author_ids")),
                    "author_afids": first_value(item.get("author_afids")),
                    "issn": normalize_text(item.get("issn")),
                    "source_id": normalize_text(item.get("source_id")),
                    "eissn": normalize_text(item.get("eIssn")),
                    "aggregation_type": normalize_text(item.get("aggregationType")),
                    "volume": normalize_text(item.get("volume")),
                    "issue_identifier": normalize_text(item.get("issueIdentifier")),
                    "article_number": normalize_text(item.get("article_number")),
                    "page_range": normalize_text(item.get("pageRange")),
                    "description": normalize_text(item.get("description")),
                    "authkeywords": normalize_text(item.get("authkeywords")),
                    "openaccess": safe_int(item.get("openaccess")),
                    "freetoread": normalize_text(item.get("freetoread")),
                    "freetoread_label": normalize_text(item.get("freetoreadLabel")),
                    "fund_acr": normalize_text(item.get("fund_acr")),
                    "fund_no": normalize_text(item.get("fund_no")),
                    "fund_sponsor": normalize_text(item.get("fund_sponsor")),
                    "group": normalize_text(item.get("group")),
                    "category": normalize_text(item.get("category")),
                    "journal_clarivate": normalize_text(item.get("journal_clarivate")),
                    "source_file": jf,
                    "is_seed_top5pct": True,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(
        ["doi", "citedby_count"], ascending=[True, False], na_position="last"
    ).drop_duplicates(subset=["doi"], keep="first")
    df = df.drop_duplicates(subset=["seed_paper_id"], keep="first").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Stage 2: citation-event extraction (chunked)
# ---------------------------------------------------------------------------
def load_semantic_citation_items_chunk(
    citing_zip: Path, paths: Iterable[str]
) -> pd.DataFrame:
    """Build citation-event records for one chunk of ``citing_contexts.json`` files.

    Only items carrying both a non-empty intent list and a non-empty context
    list are retained; the first intent is taken as the primary intent.
    """
    rows = []

    for p in paths:
        bundle = parse_bundle_info(p)
        cited_doi_guess = bundle_id_to_doi(bundle["bundle_id"])

        try:
            obj = load_json_from_zip(citing_zip, p)
        except Exception:
            continue

        if not isinstance(obj, list):
            continue

        for item in obj:
            if not isinstance(item, dict):
                continue

            intents = item.get("intents", [])
            contexts = item.get("contexts", [])
            if not isinstance(intents, list) or len(intents) == 0:
                continue
            if not isinstance(contexts, list) or len(contexts) == 0:
                continue

            primary_intent = normalize_text(intents[0])
            if not primary_intent:
                continue

            citing_doi = normalize_text(item.get("doi"))
            citing_title = normalize_text(item.get("title"))
            paper_id = normalize_text(item.get("paperId"))
            if not citing_doi and not citing_title and not paper_id:
                continue

            citing_paper_id = make_citing_paper_id(citing_doi, paper_id, citing_title)
            cited_seed_paper_id = make_seed_paper_id(cited_doi_guess, None)

            rows.append(
                {
                    "citation_event_id": f"{citing_paper_id}__to__{cited_seed_paper_id}",
                    "citing_paper_id": citing_paper_id,
                    "cited_seed_paper_id": cited_seed_paper_id,
                    "citing_paper_external_id": paper_id,
                    "citing_doi": citing_doi,
                    "citing_title": citing_title,
                    "citing_year": safe_int(item.get("year")),
                    "citing_venue": normalize_text(item.get("venue")),
                    "oa_pdf": normalize_text(item.get("oa_pdf")),
                    "is_influential": bool(item.get("isInfluential", False)),
                    "all_intents": intents,
                    "primary_intent": primary_intent,
                    "contexts": contexts,
                    "context_count": len(contexts),
                    "intent_count": len(intents),
                    "field_folder": bundle["field_folder"],
                    "bundle_id": bundle["bundle_id"],
                    "cited_doi_guess": cited_doi_guess,
                    "source_file": p,
                    "has_semantic_evidence": True,
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = (
            df.sort_values(
                ["citation_event_id", "context_count", "intent_count"],
                ascending=[True, False, False],
            )
            .drop_duplicates(subset=["citation_event_id"], keep="first")
            .reset_index(drop=True)
        )
    return df


def build_citation_events(citing_zip: Path, chunk_dir: Path) -> pd.DataFrame:
    """Process every ``citing_contexts.json`` file in chunks and concatenate."""
    all_paths = [p for p in list_zip_files(citing_zip) if p.endswith("citing_contexts.json")]
    print(f"total citing_contexts.json files: {len(all_paths)}")

    n_chunks = math.ceil(len(all_paths) / CHUNK_SIZE) if all_paths else 0
    print(f"total chunks: {n_chunks}")

    for i in range(n_chunks):
        start = i * CHUNK_SIZE
        end = min((i + 1) * CHUNK_SIZE, len(all_paths))
        out_path = chunk_dir / f"citation_events_chunk_{i:05d}.parquet"

        if out_path.exists():
            print(f"[skip] chunk {i + 1}/{n_chunks} already saved -> {out_path.name}")
            continue

        print(f"[run]  chunk {i + 1}/{n_chunks} | files {start}:{end}")
        chunk_df = load_semantic_citation_items_chunk(citing_zip, all_paths[start:end])
        chunk_df.to_parquet(out_path, index=False)
        print(f"       saved {out_path.name} | shape={chunk_df.shape}")

    chunk_files = sorted(chunk_dir.glob("citation_events_chunk_*.parquet"))
    print(f"saved chunk files: {len(chunk_files)}")
    if not chunk_files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in chunk_files], ignore_index=True)
    df = (
        df.sort_values(
            ["citation_event_id", "context_count", "intent_count"],
            ascending=[True, False, False],
        )
        .drop_duplicates(subset=["citation_event_id"], keep="first")
        .reset_index(drop=True)
    )
    return df


# ---------------------------------------------------------------------------
# Stage 3: publication tables and enrichment
# ---------------------------------------------------------------------------
def build_citing_papers_df(citation_events_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "citing_paper_id",
        "citing_paper_external_id",
        "citing_doi",
        "citing_title",
        "citing_year",
        "citing_venue",
        "oa_pdf",
    ]
    df = (
        citation_events_df[cols]
        .copy()
        .drop_duplicates(subset=["citing_paper_id"], keep="first")
        .rename(
            columns={
                "citing_paper_external_id": "paperId",
                "citing_doi": "doi",
                "citing_title": "title",
                "citing_year": "year",
                "citing_venue": "venue",
            }
        )
        .reset_index(drop=True)
    )
    return df


def attach_seed_metadata(
    citation_events_df: pd.DataFrame, seed_cited_papers_df: pd.DataFrame
) -> pd.DataFrame:
    seed_cols = [
        "seed_paper_id",
        "doi",
        "title",
        "publication_name",
        "creator",
        "affilname",
        "affiliation_city",
        "affiliation_country",
        "group",
        "category",
        "journal_clarivate",
        "citedby_count",
    ]
    seed_lookup = seed_cited_papers_df[seed_cols].rename(
        columns={
            "doi": "cited_doi",
            "title": "cited_title",
            "publication_name": "cited_publication_name",
            "creator": "cited_creator",
            "affilname": "cited_affilname",
            "affiliation_city": "cited_affiliation_city",
            "affiliation_country": "cited_affiliation_country",
            "group": "cited_group",
            "category": "cited_category",
            "journal_clarivate": "cited_journal_clarivate",
            "citedby_count": "cited_citedby_count",
        }
    )
    return citation_events_df.merge(
        seed_lookup,
        left_on="cited_seed_paper_id",
        right_on="seed_paper_id",
        how="left",
        suffixes=("", "_seed"),
    )


# ---------------------------------------------------------------------------
# Stage 4: entity normalization
# ---------------------------------------------------------------------------
def build_lookup_table(
    series: pd.Series, id_prefix: str, id_col: str, value_col: str
) -> pd.DataFrame:
    vals = (
        pd.Series(series)
        .dropna()
        .map(clean_str)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    df = pd.DataFrame({value_col: vals})
    df[id_col] = [f"{id_prefix}:{i + 1:06d}" for i in range(len(df))]
    return df[[id_col, value_col]]


def normalize_field_folder(x: Any) -> Optional[str]:
    x = clean_str(x)
    if x is None:
        return None
    if x.startswith("output_"):
        x = x.replace("output_", "")
    return x.replace("_", " ")


# ---------------------------------------------------------------------------
# Stage 5: knowledge-graph construction
# ---------------------------------------------------------------------------
def build_node_df(df, node_id_col, label_col, node_type, extra_cols=None):
    extra_cols = extra_cols or []
    cols = [node_id_col, label_col] + [c for c in extra_cols if c in df.columns]
    out = df[cols].copy().drop_duplicates(subset=[node_id_col])
    out = out.rename(columns={node_id_col: "node_id", label_col: "label"})
    out["node_type"] = node_type
    return out


def build_edge_df(df, source_col, target_col, edge_type, extra_cols=None):
    extra_cols = extra_cols or []
    use_cols = [source_col, target_col] + [c for c in extra_cols if c in df.columns]
    out = df[use_cols].copy().rename(columns={source_col: "source", target_col: "target"})
    out["edge_type"] = edge_type
    return out.dropna(subset=["source", "target"]).drop_duplicates().reset_index(drop=True)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_pipeline(base_dir: Path, cited_zip_name: str, citing_zip_name: str) -> None:
    cited_zip = base_dir / cited_zip_name
    citing_zip = base_dir / citing_zip_name
    out_dir = base_dir / OUTPUT_SUBDIR
    chunk_dir = out_dir / "citation_event_chunks"
    out_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    for required in (cited_zip, citing_zip):
        if not required.exists():
            raise FileNotFoundError(f"Required input not found: {required}")

    # Stage 1 -- seed (cited) papers
    print("\n[1/5] Loading seed (cited) papers ...")
    seed_cited_papers_df = load_seed_cited_papers(cited_zip)
    print(f"seed_cited_papers_df: {seed_cited_papers_df.shape}")
    seed_cited_papers_df.to_parquet(out_dir / "seed_cited_papers.parquet", index=False)

    # Stage 2 -- citation events
    print("\n[2/5] Building citation events ...")
    citation_events_df = build_citation_events(citing_zip, chunk_dir)
    print(f"citation_events_df: {citation_events_df.shape}")
    citation_events_df.to_parquet(out_dir / "citation_events.parquet", index=False)

    # Stage 3 -- citing papers + enrichment
    print("\n[3/5] Building citing-paper and enriched tables ...")
    citing_papers_df = build_citing_papers_df(citation_events_df)
    citing_papers_df.to_parquet(out_dir / "citing_papers.parquet", index=False)
    print(f"citing_papers_df: {citing_papers_df.shape}")

    citation_events_enriched_df = attach_seed_metadata(
        citation_events_df, seed_cited_papers_df
    )
    citation_events_enriched_df.to_parquet(
        out_dir / "citation_events_enriched.parquet", index=False
    )
    print(f"citation_events_enriched_df: {citation_events_enriched_df.shape}")

    # Stage 4 -- normalized entity lookup tables
    print("\n[4/5] Normalizing entities ...")
    journal_name_series = pd.concat(
        [
            seed_cited_papers_df.get("publication_name", pd.Series(dtype=object)),
            seed_cited_papers_df.get("journal_clarivate", pd.Series(dtype=object)),
            citing_papers_df.get("venue", pd.Series(dtype=object)),
        ],
        ignore_index=True,
    )
    journals_df = build_lookup_table(journal_name_series, "journal", "journal_id", "journal_name")
    authors_df = build_lookup_table(
        seed_cited_papers_df.get("creator", pd.Series(dtype=object)),
        "author", "author_id", "author_name",
    )
    countries_df = build_lookup_table(
        seed_cited_papers_df.get("affiliation_country", pd.Series(dtype=object)),
        "country", "country_id", "country_name",
    )
    cities_df = build_lookup_table(
        seed_cited_papers_df.get("affiliation_city", pd.Series(dtype=object)),
        "city", "city_id", "city_name",
    )
    affiliations_df = build_lookup_table(
        seed_cited_papers_df.get("affilname", pd.Series(dtype=object)),
        "affiliation", "affiliation_id", "affiliation_name",
    )

    fields_base = (
        seed_cited_papers_df[["group", "category"]]
        .rename(columns={"group": "field_name", "category": "category_raw"})
        .copy()
    )
    fields_base["field_name"] = fields_base["field_name"].map(clean_str)
    fields_base["category_raw"] = fields_base["category_raw"].map(clean_str)
    fields_df = (
        fields_base.dropna(subset=["field_name"])
        .drop_duplicates()
        .sort_values(["field_name", "category_raw"], na_position="last")
        .reset_index(drop=True)
    )
    fields_df["field_id"] = [f"field:{i + 1:06d}" for i in range(len(fields_df))]
    fields_df = fields_df[["field_id", "field_name", "category_raw"]]

    intents_df = build_lookup_table(
        citation_events_df.get("primary_intent", pd.Series(dtype=object)),
        "intent", "intent_id", "intent_name",
    )

    journal_map = dict(zip(journals_df["journal_name"], journals_df["journal_id"]))
    author_map = dict(zip(authors_df["author_name"], authors_df["author_id"]))
    affiliation_map = dict(zip(affiliations_df["affiliation_name"], affiliations_df["affiliation_id"]))
    city_map = dict(zip(cities_df["city_name"], cities_df["city_id"]))
    country_map = dict(zip(countries_df["country_name"], countries_df["country_id"]))
    intent_map = dict(zip(intents_df["intent_name"], intents_df["intent_id"]))
    field_map = dict(zip(fields_df["field_name"], fields_df["field_id"]))

    seed_norm = seed_cited_papers_df.copy()
    seed_norm["journal_id"] = seed_norm["publication_name"].map(clean_str).map(journal_map)
    seed_norm["author_id"] = seed_norm["creator"].map(clean_str).map(author_map)
    seed_norm["affiliation_id"] = seed_norm["affilname"].map(clean_str).map(affiliation_map)
    seed_norm["city_id"] = seed_norm["affiliation_city"].map(clean_str).map(city_map)
    seed_norm["country_id"] = seed_norm["affiliation_country"].map(clean_str).map(country_map)
    seed_norm["field_id"] = seed_norm["group"].map(clean_str).map(field_map)

    citing_norm = citing_papers_df.copy()
    citing_norm["journal_id"] = citing_norm["venue"].map(clean_str).map(journal_map)

    field_name_norm_map = {
        clean_str(name): fid for name, fid in zip(fields_df["field_name"], fields_df["field_id"])
    }
    events_norm = citation_events_df.copy()
    events_norm["intent_id"] = events_norm["primary_intent"].map(clean_str).map(intent_map)
    events_norm["field_name_from_folder"] = events_norm["field_folder"].map(normalize_field_folder)
    events_norm["field_id"] = events_norm["field_name_from_folder"].map(field_name_norm_map)

    affiliation_geo_df = (
        seed_cited_papers_df[["affilname", "affiliation_city", "affiliation_country"]]
        .rename(
            columns={
                "affilname": "affiliation_name",
                "affiliation_city": "city_name",
                "affiliation_country": "country_name",
            }
        )
        .copy()
    )
    for col in ("affiliation_name", "city_name", "country_name"):
        affiliation_geo_df[col] = affiliation_geo_df[col].map(clean_str)
    affiliation_geo_df = (
        affiliation_geo_df.dropna(subset=["affiliation_name"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    affiliation_geo_df["affiliation_id"] = affiliation_geo_df["affiliation_name"].map(affiliation_map)
    affiliation_geo_df["city_id"] = affiliation_geo_df["city_name"].map(city_map)
    affiliation_geo_df["country_id"] = affiliation_geo_df["country_name"].map(country_map)

    journals_df.to_parquet(out_dir / "journals.parquet", index=False)
    authors_df.to_parquet(out_dir / "authors.parquet", index=False)
    affiliations_df.to_parquet(out_dir / "affiliations.parquet", index=False)
    cities_df.to_parquet(out_dir / "cities.parquet", index=False)
    countries_df.to_parquet(out_dir / "countries.parquet", index=False)
    fields_df.to_parquet(out_dir / "fields.parquet", index=False)
    intents_df.to_parquet(out_dir / "intents.parquet", index=False)
    seed_norm.to_parquet(out_dir / "seed_cited_papers_normalized.parquet", index=False)
    citing_norm.to_parquet(out_dir / "citing_papers_normalized.parquet", index=False)
    events_norm.to_parquet(out_dir / "citation_events_normalized.parquet", index=False)
    affiliation_geo_df.to_parquet(out_dir / "affiliation_geo.parquet", index=False)
    print("saved normalized / lookup parquet files")

    # Stage 5 -- knowledge graph
    print("\n[5/5] Building knowledge graph ...")
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
        description="Build the ontology-ready CitationHub resource from raw inputs."
    )
    parser.add_argument(
        "--base-dir",
        required=True,
        type=Path,
        help="Directory containing the seed and citing input ZIP archives.",
    )
    parser.add_argument(
        "--cited-zip",
        default=DEFAULT_CITED_ZIP,
        help="File name of the seed (cited) paper ZIP archive.",
    )
    parser.add_argument(
        "--citing-zip",
        default=DEFAULT_CITING_ZIP,
        help="File name of the citing-context ZIP archive.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(args.base_dir, args.cited_zip, args.citing_zip)


if __name__ == "__main__":
    main()
