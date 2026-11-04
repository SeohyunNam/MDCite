"""Compare the IDCite v3 and v4 releases.

Checks that v4 is free of Scopus-derived fields and reports the effect of the
OpenAlex metadata replacement:

    1. Row and column counts of every v4 table against v3, and any remaining
       column whose name matches a Scopus field.
    2. Seed-paper metadata coverage after the OpenAlex replacement.
    3. Citation-event linkage to the seed papers (unchanged from v3).
    4. Identifier uniqueness of the rebuilt entity tables and the knowledge
       graph, and the knowledge-graph node-type counts.

Usage
-----
    python compare_v3_v4.py --v3-dir /path/to/v3 --v4-dir /path/to/IDCite_v4
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCOPUS_FIELDS = {
    "eid", "afid", "source_id", "pii", "subtype", "subtype_description", "cover_date",
    "cover_display_date", "author_ids", "author_afids", "author_names", "authkeywords",
    "description", "freetoread", "freetoread_label", "fund_acr", "fund_no", "fund_sponsor",
    "aggregation_type", "eissn", "issue_identifier", "page_range", "article_number",
    "openaccess", "pubmed_id",
}
SEED_COVERAGE_COLS = [
    "openalex_id", "doi", "title", "publication_year", "publication_name", "creator",
    "affilname", "affiliation_country", "citedby_count",
]


def compare_tables(v3_dir: Path, v4_dir: Path) -> None:
    print("table | rows v3 -> v4 | columns v3 -> v4 | Scopus-named columns")
    for path in sorted(v4_dir.glob("*.parquet")):
        cols4 = pq.read_schema(path).names
        rows4 = pq.ParquetFile(path).metadata.num_rows
        v3_path = v3_dir / path.name
        rows3 = pq.ParquetFile(v3_path).metadata.num_rows if v3_path.exists() else None
        cols3 = len(pq.read_schema(v3_path).names) if v3_path.exists() else None
        bad = sorted(SCOPUS_FIELDS & set(cols4)) or "-"
        print(f"{path.name} | {rows3} -> {rows4} | {cols3} -> {len(cols4)} | {bad}")


def seed_coverage(v4_dir: Path) -> pd.DataFrame:
    seed = pd.read_parquet(v4_dir / "seed_cited_papers.parquet")
    print("\nseed coverage (%): " + ", ".join(
        f"{c}={seed[c].notna().mean() * 100:.2f}" for c in SEED_COVERAGE_COLS
    ))
    print(f"seed papers matched in OpenAlex: {seed['openalex_id'].notna().sum()} / {len(seed)}")
    return seed


def seed_linkage(v4_dir: Path, seed: pd.DataFrame) -> None:
    events = pd.read_parquet(v4_dir / "citation_events.parquet", columns=["cited_seed_paper_id"])
    linked = events["cited_seed_paper_id"].isin(set(seed["seed_paper_id"]))
    unresolved = events.loc[~linked, "cited_seed_paper_id"]
    print(f"events linked to seed papers: {linked.mean() * 100:.2f}% | "
          f"unresolved events: {len(unresolved)} | unresolved ids: {unresolved.nunique()}")


def uniqueness(v4_dir: Path) -> None:
    for table, id_col in [("authors", "author_id"), ("affiliations", "affiliation_id"),
                          ("journals", "journal_id")]:
        df = pd.read_parquet(v4_dir / f"{table}.parquet")
        print(f"{table}: duplicate ids = {df[id_col].duplicated().sum()}")
    nodes = pd.read_parquet(v4_dir / "kg_nodes.parquet", columns=["node_id", "node_type"])
    print(f"kg_nodes: duplicate ids = {nodes['node_id'].duplicated().sum()}")
    print(nodes["node_type"].value_counts().to_string())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare the IDCite v3 and v4 releases.")
    parser.add_argument("--v3-dir", required=True, type=Path, help="Directory with v3 tables.")
    parser.add_argument("--v4-dir", required=True, type=Path, help="Directory with v4 tables.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    compare_tables(args.v3_dir, args.v4_dir)
    seed = seed_coverage(args.v4_dir)
    seed_linkage(args.v4_dir, seed)
    uniqueness(args.v4_dir)


if __name__ == "__main__":
    main()
