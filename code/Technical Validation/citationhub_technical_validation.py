"""Technical validation for the IDCite resource.

This script reproduces the technical-validation analyses reported in the
IDCite Scientific Data paper (Section 3). Given the ontology-ready
Parquet tables produced by ``ontology.py``, it computes:

    1. Metadata completeness for the citation-event, citing-paper, and
       seed-paper tables (Section 3.1, Table 8).
    2. Citation-event referential integrity against the seed and citing
       paper tables (Section 3.3).
    3. The canonical citation-intent distribution, obtained by decomposing
       composite intent labels into their constituent intents
       (Section 2.5 / Table 4 / Fig. 3), together with an intent-coverage
       summary (Section 3.2).
    4. Knowledge-graph integrity statistics: node/edge counts, duplicate
       node identifiers, and source/target referential integrity
       (Section 3.3, Table 9).
    5. Entity-table uniqueness checks for the normalized lookup tables.

All result tables are written as CSV files and bundled into a single ZIP
archive. An optional citation-intent figure (Fig. 3) can be produced with
``--make-figures``.

Usage
-----
    python citationhub_technical_validation.py --data-dir /path/to/citationhub_v1_ontology_ready

The ``--data-dir`` argument must point to the directory containing the
``*.parquet`` tables (i.e. ``<base-dir>/citationhub_v1_ontology_ready``).
"""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path
from typing import List

import pandas as pd

CANONICAL_INTENTS = [
    "background",
    "uses",
    "motivation",
    "extends",
    "similarities",
    "differences",
    "future_work",
]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def read_parquet(data_dir: Path, name: str) -> pd.DataFrame:
    path = data_dir / name
    if not path.exists():
        raise FileNotFoundError(f"Required table not found: {path}")
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# 1. Metadata completeness (Table 8)
# ---------------------------------------------------------------------------
def completeness(df: pd.DataFrame, columns: List[str], table_name: str) -> pd.DataFrame:
    n = len(df)
    rows = []
    for col in columns:
        non_null = int(df[col].notna().sum()) if col in df.columns else 0
        rows.append(
            {
                "Table": table_name,
                "Column": col,
                "Non-null": non_null,
                "Total": n,
                "Completeness (%)": round(non_null / n * 100, 3) if n else 0.0,
            }
        )
    return pd.DataFrame(rows)


def metadata_completeness(ce: pd.DataFrame, cp: pd.DataFrame, sp: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(
        [
            completeness(
                ce,
                [
                    "citation_event_id", "citing_paper_id", "cited_seed_paper_id",
                    "citing_doi", "citing_title", "citing_year", "citing_venue",
                    "primary_intent", "context_count", "intent_count",
                ],
                "citation_events",
            ),
            completeness(
                cp,
                ["citing_paper_id", "doi", "title", "year", "venue", "oa_pdf"],
                "citing_papers",
            ),
            completeness(
                sp,
                [
                    "seed_paper_id", "doi", "title", "publication_name", "creator",
                    "citedby_count", "affilname", "affiliation_country", "group", "category",
                ],
                "seed_cited_papers",
            ),
        ],
        ignore_index=True,
    )


# ---------------------------------------------------------------------------
# 2. Citation-event referential integrity
# ---------------------------------------------------------------------------
def event_integrity(ce: pd.DataFrame, cp: pd.DataFrame, sp: pd.DataFrame) -> pd.DataFrame:
    seed_ids = set(sp["seed_paper_id"].dropna())
    citing_ids = set(cp["citing_paper_id"].dropna())
    seed_match = int(ce["cited_seed_paper_id"].isin(seed_ids).sum())
    citing_match = int(ce["citing_paper_id"].isin(citing_ids).sum())
    n = len(ce)
    return pd.DataFrame(
        [
            {
                "Check": "cited_seed_paper_id linked to seed_cited_papers",
                "Matched": seed_match,
                "Total": n,
                "Match rate (%)": round(seed_match / n * 100, 3) if n else 0.0,
                "Unmatched": n - seed_match,
            },
            {
                "Check": "citing_paper_id linked to citing_papers",
                "Matched": citing_match,
                "Total": n,
                "Match rate (%)": round(citing_match / n * 100, 3) if n else 0.0,
                "Unmatched": n - citing_match,
            },
        ]
    )


# ---------------------------------------------------------------------------
# 3. Citation-intent distribution and coverage (Table 4 / Fig. 3)
# ---------------------------------------------------------------------------
def raw_intent_distribution(ce: pd.DataFrame) -> pd.DataFrame:
    counts = ce["primary_intent"].fillna("None").value_counts().reset_index()
    counts.columns = ["Intent label", "Count"]
    counts["Percentage (%)"] = (counts["Count"] / len(ce) * 100).round(3)
    return counts


def canonical_intent_distribution(ce: pd.DataFrame) -> pd.DataFrame:
    """Decompose composite labels into the seven canonical intents (Table 4)."""
    primary = ce["primary_intent"].fillna("")
    rows = []
    for intent in CANONICAL_INTENTS:
        rows.append({"Intent": intent, "Count": int(primary.str.contains(intent, regex=False).sum())})
    df = pd.DataFrame(rows).sort_values("Count", ascending=False).reset_index(drop=True)
    total = df["Count"].sum()
    df["Percentage (%)"] = (df["Count"] / total * 100).round(2) if total else 0.0
    return df


def intent_summary(ce: pd.DataFrame, intents: pd.DataFrame) -> pd.DataFrame:
    valid = int(ce["primary_intent"].notna().sum())
    missing = int(ce["primary_intent"].isna().sum())
    return pd.DataFrame(
        [
            {"Metric": "Total citation events", "Value": len(ce)},
            {"Metric": "Events with valid primary_intent", "Value": valid},
            {"Metric": "Events with missing primary_intent", "Value": missing},
            {"Metric": "Observed raw intent labels (incl. missing)",
             "Value": int(ce["primary_intent"].fillna("None").nunique())},
            {"Metric": "Valid intent labels (excl. missing)",
             "Value": int(ce["primary_intent"].dropna().nunique())},
            {"Metric": "Rows in intents.parquet", "Value": len(intents)},
        ]
    )


# ---------------------------------------------------------------------------
# 4. Knowledge-graph integrity (Table 9)
# ---------------------------------------------------------------------------
def kg_integrity(kg_nodes: pd.DataFrame, kg_edges: pd.DataFrame) -> pd.DataFrame:
    node_ids = set(kg_nodes["node_id"].dropna())
    source_match = int(kg_edges["source"].isin(node_ids).sum())
    target_match = int(kg_edges["target"].isin(node_ids).sum())
    n_edges = len(kg_edges)
    return pd.DataFrame(
        [
            {"Metric": "Total KG nodes", "Value": len(kg_nodes)},
            {"Metric": "Total KG edges", "Value": n_edges},
            {"Metric": "Unique node_id count", "Value": int(kg_nodes["node_id"].nunique())},
            {"Metric": "Duplicate node_id count",
             "Value": len(kg_nodes) - int(kg_nodes["node_id"].nunique())},
            {"Metric": "Edges with valid source node", "Value": source_match},
            {"Metric": "Source node match rate (%)",
             "Value": round(source_match / n_edges * 100, 3) if n_edges else 0.0},
            {"Metric": "Edges with valid target node", "Value": target_match},
            {"Metric": "Target node match rate (%)",
             "Value": round(target_match / n_edges * 100, 3) if n_edges else 0.0},
            {"Metric": "Edges with missing source", "Value": n_edges - source_match},
            {"Metric": "Edges with missing target", "Value": n_edges - target_match},
        ]
    )


def edge_type_distribution(kg_edges: pd.DataFrame) -> pd.DataFrame:
    dist = kg_edges["edge_type"].value_counts().reset_index()
    dist.columns = ["Edge type", "Count"]
    dist["Percentage (%)"] = (dist["Count"] / len(kg_edges) * 100).round(3)
    return dist


# ---------------------------------------------------------------------------
# 5. Entity-table uniqueness
# ---------------------------------------------------------------------------
def entity_uniqueness(data_dir: Path) -> pd.DataFrame:
    entity_checks = [
        ("authors", "authors.parquet", "author_id"),
        ("affiliations", "affiliations.parquet", "affiliation_id"),
        ("journals", "journals.parquet", "journal_id"),
        ("fields", "fields.parquet", "field_id"),
        ("intents", "intents.parquet", "intent_id"),
        ("countries", "countries.parquet", "country_id"),
        ("cities", "cities.parquet", "city_id"),
    ]
    rows = []
    for name, fname, id_col in entity_checks:
        path = data_dir / fname
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        rows.append(
            {
                "Entity table": name,
                "Rows": len(df),
                "Unique IDs": int(df[id_col].nunique()),
                "Duplicate IDs": len(df) - int(df[id_col].nunique()),
                "Missing IDs": int(df[id_col].isna().sum()),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Optional figure (Fig. 3)
# ---------------------------------------------------------------------------
def save_intent_figure(canonical_df: pd.DataFrame, out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ordered = canonical_df.sort_values("Count", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(ordered["Intent"], ordered["Count"])
    ax.set_ylabel("Number of intent occurrences")
    ax.set_xlabel("Citation intent")
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h, f"{int(h):,}",
                ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=600, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_validation(data_dir: Path, out_dir: Path, make_figures: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    ce = read_parquet(data_dir, "citation_events.parquet")
    cp = read_parquet(data_dir, "citing_papers.parquet")
    sp = read_parquet(data_dir, "seed_cited_papers.parquet")
    kg_nodes = read_parquet(data_dir, "kg_nodes.parquet")
    kg_edges = read_parquet(data_dir, "kg_edges.parquet")
    intents = read_parquet(data_dir, "intents.parquet")

    results = {
        "technical_validation_metadata_completeness.csv": metadata_completeness(ce, cp, sp),
        "technical_validation_event_integrity.csv": event_integrity(ce, cp, sp),
        "technical_validation_intent_distribution_raw.csv": raw_intent_distribution(ce),
        "technical_validation_intent_distribution_canonical.csv": canonical_intent_distribution(ce),
        "technical_validation_intent_summary.csv": intent_summary(ce, intents),
        "technical_validation_kg_integrity.csv": kg_integrity(kg_nodes, kg_edges),
        "technical_validation_edge_type_distribution.csv": edge_type_distribution(kg_edges),
        "technical_validation_entity_uniqueness.csv": entity_uniqueness(data_dir),
    }

    written = []
    for fname, df in results.items():
        out_path = out_dir / fname
        df.to_csv(out_path, index=False)
        written.append(out_path)
        print(f"\n=== {fname} ===")
        print(df.to_string(index=False))

    if make_figures:
        fig_path = out_dir / "intent_distribution.png"
        save_intent_figure(results["technical_validation_intent_distribution_canonical.csv"], fig_path)
        print(f"\nsaved figure: {fig_path}")

    zip_path = out_dir / "citationhub_technical_validation_results.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for path in written:
            z.write(path, arcname=path.name)
    print(f"\nSaved bundle: {zip_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run technical validation on the IDCite Parquet tables."
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        type=Path,
        help="Directory containing the IDCite *.parquet tables.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory for validation outputs (defaults to <data-dir>/technical_validation).",
    )
    parser.add_argument(
        "--make-figures",
        action="store_true",
        help="Also render the citation-intent distribution figure (Fig. 3).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = args.out_dir or (args.data_dir / "technical_validation")
    run_validation(args.data_dir, out_dir, args.make_figures)


if __name__ == "__main__":
    main()
