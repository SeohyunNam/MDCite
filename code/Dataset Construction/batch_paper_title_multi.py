"""Batch driver for large-scale citation context extraction.

This script wraps :func:`paper_title.process_one` to extract citation contexts
for many seed papers. It supports two modes:

    single   Process one CSV file of seed papers into one output directory.
    multi    Process every ``*_top5pct_per_journal_for_paper_title.csv`` file
             found in a directory, writing per-group output subdirectories.

Each input CSV is expected to contain at least ``title`` and ``doi`` columns.

Usage
-----
    # Single CSV
    python batch_paper_title_multi.py --mode single \\
        --csv seeds.csv --out output_seeds

    # All per-journal group CSVs in a directory
    python batch_paper_title_multi.py --mode multi \\
        --csv-dir group_top5pct_per_journal --out-base-dir output_groups
"""

import os
import time
import math
import glob
import argparse

import pandas as pd

from paper_title import process_one

DEFAULT_PATTERN = "*_top5pct_per_journal_for_paper_title.csv"


def clean_doi(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    if not value:
        return None
    return str(value).strip()


def run_for_one(csv_path, out_dir, *, limit, sleep_sec, fetch_openalex,
                fetch_scopus, scopus_year_range):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(csv_path)
    n = len(df)
    print(f"\n=== CSV: {csv_path} ===")
    print(f"Total {n} papers to process...")

    for idx, row in df.iterrows():
        title = row.get("title")
        doi = clean_doi(row.get("doi"))

        if not title and not doi:
            print(f"[{idx + 1}/{n}] Missing title/doi -> skip")
            continue

        print("\n==========================================")
        print(f"[{idx + 1}/{n}]")
        print("TITLE:", title)
        print("DOI  :", doi)

        try:
            process_one(
                title=title,
                doi=doi,
                outdir=out_dir,
                limit=limit,
                fetch_openalex=fetch_openalex,
                fetch_scopus=fetch_scopus,
                scopus_year_range=scopus_year_range,
            )
        except Exception as e:
            print(f"[{idx + 1}/{n}] Error -> skip: {e}")

        time.sleep(sleep_sec)

    print(f"\nDone for CSV: {csv_path}")


def run_for_all_groups(csv_dir, out_base_dir, *, pattern, limit, sleep_sec,
                       fetch_openalex, fetch_scopus, scopus_year_range):
    os.makedirs(out_base_dir, exist_ok=True)

    search = os.path.join(csv_dir, pattern)
    csv_files = sorted(glob.glob(search))

    if not csv_files:
        print(f"[warn] No CSV files found with pattern: {search}")
        return

    print(f"Found {len(csv_files)} group CSV files.")
    suffix = "_top5pct_per_journal_for_paper_title.csv"
    for csv_path in csv_files:
        base = os.path.basename(csv_path)
        group_key = base.replace(suffix, "")
        out_dir = os.path.join(out_base_dir, f"output_{group_key}")

        print("\n==========================================")
        print(f"Group: {group_key}")
        print(f"   CSV : {csv_path}")
        print(f"   OUT : {out_dir}")

        run_for_one(
            csv_path, out_dir,
            limit=limit, sleep_sec=sleep_sec,
            fetch_openalex=fetch_openalex, fetch_scopus=fetch_scopus,
            scopus_year_range=scopus_year_range,
        )

    print("\n=== All groups processed. ===")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch citation context extraction for seed papers."
    )
    parser.add_argument("--mode", choices=["single", "multi"], default="multi")
    parser.add_argument("--csv", help="Input CSV path (single mode).")
    parser.add_argument("--out", help="Output directory (single mode).")
    parser.add_argument("--csv-dir", help="Directory of group CSV files (multi mode).")
    parser.add_argument("--out-base-dir", help="Base output directory (multi mode).")
    parser.add_argument("--pattern", default=DEFAULT_PATTERN,
                        help="Glob pattern for group CSVs (multi mode).")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max citation contexts per paper (0 = no limit).")
    parser.add_argument("--sleep", type=float, default=1.0,
                        help="Delay in seconds between papers.")
    parser.add_argument("--fetch-openalex", dest="fetch_openalex",
                        action="store_true", default=True,
                        help="Also save the OpenAlex citing list (default on).")
    parser.add_argument("--no-fetch-openalex", dest="fetch_openalex",
                        action="store_false")
    parser.add_argument("--fetch-scopus", dest="fetch_scopus",
                        action="store_true", default=False,
                        help="Attempt Scopus full citing retrieval (requires entitlement).")
    parser.add_argument("--scopus-year-range", default=None,
                        help="Optional Scopus year range filter.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.mode == "single":
        if not args.csv or not args.out:
            raise SystemExit("single mode requires --csv and --out")
        run_for_one(
            args.csv, args.out,
            limit=args.limit, sleep_sec=args.sleep,
            fetch_openalex=args.fetch_openalex, fetch_scopus=args.fetch_scopus,
            scopus_year_range=args.scopus_year_range,
        )
    else:
        if not args.csv_dir or not args.out_base_dir:
            raise SystemExit("multi mode requires --csv-dir and --out-base-dir")
        run_for_all_groups(
            args.csv_dir, args.out_base_dir,
            pattern=args.pattern, limit=args.limit, sleep_sec=args.sleep,
            fetch_openalex=args.fetch_openalex, fetch_scopus=args.fetch_scopus,
            scopus_year_range=args.scopus_year_range,
        )


if __name__ == "__main__":
    main()
