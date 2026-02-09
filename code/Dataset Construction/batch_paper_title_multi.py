
import os
import time
import math
import glob
import pandas as pd
from paper_title import process_one  # import your existing script


MODE = "multi"   

CSV_PATH = r"C:\Users\user\pybliometrics_ml\lancet_top5_for_paper_title.csv"
OUT_DIR  = r"C:\Users\user\pybliometrics_ml\output_lancet_top5"

CSV_DIR = r"C:\Users\user\OneDrive\바탕 화면\wos dataset\group_top5pct_per_journal\group_top5pct_per_journal"
OUT_BASE_DIR = r"C:\Users\user\pybliometrics_ml\output_group_top5pct_per_journal"

SLEEP_SEC = 1.0
LIMIT = 0                  
FETCH_OPENALEX = True
FETCH_SCOPUS   = False
SCOPUS_YEAR_RANGE = None  


def clean_doi(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    if not value:
        return None
    return str(value).strip()


def run_for_one(csv_path, out_dir):
    
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(csv_path)
    n = len(df)
    print(f"\n=== CSV: {csv_path} ===")
    print(f"Total {n} papers to process...")

    for idx, row in df.iterrows():
        title = row.get("title")
        doi = clean_doi(row.get("doi"))

        if not title and not doi:
            print(f"[{idx+1}/{n}] Missing title/doi → skip")
            continue

        print("\n==========================================")
        print(f"[{idx+1}/{n}]")
        print("TITLE:", title)
        print("DOI  :", doi)

        try:
            process_one(
                title=title,
                doi=doi,
                outdir=out_dir,
                limit=LIMIT,
                fetch_openalex=FETCH_OPENALEX,
                fetch_scopus=FETCH_SCOPUS,
                scopus_year_range=SCOPUS_YEAR_RANGE,
            )
        except Exception as e:
            print(f"[{idx+1}/{n}] Error → skip: {e}")

        time.sleep(SLEEP_SEC)

    print(f"\nDone for CSV: {csv_path}")


def run_for_all_groups():
    
    os.makedirs(OUT_BASE_DIR, exist_ok=True)

    pattern = os.path.join(CSV_DIR, "*_top5pct_per_journal_for_paper_title.csv")
    csv_files = sorted(glob.glob(pattern))

    if not csv_files:
        print(f"⚠ No CSV files found with pattern: {pattern}")
        return

    print(f"Found {len(csv_files)} group CSV files.")
    for csv_path in csv_files:
        base = os.path.basename(csv_path)
        group_key = base.replace("_top5pct_per_journal_for_paper_title.csv", "")
        out_dir = os.path.join(OUT_BASE_DIR, f"output_{group_key}")

        print("\n==========================================")
        print(f"▶ 그룹: {group_key}")
        print(f"   CSV : {csv_path}")
        print(f"   OUT : {out_dir}")

        run_for_one(csv_path, out_dir)

    print("\n=== All groups processed. ===")


def main():
    if MODE == "single":
        run_for_one(CSV_PATH, OUT_DIR)
    elif MODE == "multi":
        run_for_all_groups()
    else:
        raise ValueError("MODE must be 'single' or 'multi'")


if __name__ == "__main__":
    main()
