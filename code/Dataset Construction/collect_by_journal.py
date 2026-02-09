
import os
import csv
import argparse
from typing import List, Optional

import pandas as pd

def ensure_scopus_init():
    try:
        import pybliometrics
        from pybliometrics.utils import init as _init
        api = os.getenv("SCOPUS_APIKEY")
        inst = os.getenv("SCOPUS_INSTTOKEN")
        if api:
            if inst:
                pybliometrics.init(keys=[api], inst_tokens=[inst])
            else:
                pybliometrics.init(keys=[api])
        else:
            try:
                _init()
            except Exception:
                pass
    except Exception as e:
        print("[warn] pybliometrics init skipped:", e)

def scopus_search_safe(query: str, view: str = "STANDARD", download: bool = True) -> List[dict]:
    try:
        from pybliometrics.scopus import ScopusSearch
    except Exception as e:
        print("[error] Cannot import ScopusSearch:", e)
        return []

    try:
        s = ScopusSearch(query, view=view, download=download, cursor="*", count=200)
        rows = s.results or []
        return _rows_to_dicts(rows)
    except TypeError:
        pass
    except Exception as e:
        print("[info] cursor-mode failed -> fallback:", e)

    try:
        s = ScopusSearch(query, view=view, download=download)
        rows = s.results or []
        return _rows_to_dicts(rows)
    except Exception as e:
        print("[error] ScopusSearch failed:", e)
        return []

def _rows_to_dicts(rows) -> List[dict]:
    dicts = []
    for r in rows:
        if hasattr(r, "_asdict"):
            dicts.append(r._asdict())
        else:
            try:
                dicts.append(vars(r))
            except Exception:
                dicts.append({"raw": str(r)})
    return dicts

def build_base_clause(journal: Optional[str], issn: Optional[str]) -> str:
    if issn:
        return f'ISSN({issn})'
    if journal:
        return f'SRCTITLE("{journal}")'
    raise ValueError("Provide at least --journal or --issn")

def build_year_clause(year_from: Optional[int], year_to: Optional[int]) -> str:
    if year_from is None and year_to is None:
        return ""
    if year_from is not None and year_to is not None:
        return f'AND PUBYEAR AFT {int(year_from) - 1} AND PUBYEAR BEF {int(year_to) + 1}'
    if year_from is not None:
        return f'AND PUBYEAR AFT {int(year_from) - 1}'
    if year_to is not None:
        return f'AND PUBYEAR BEF {int(year_to) + 1}'
    return ""

def collect_by_journal(journal: Optional[str],
                       issn: Optional[str],
                       year_from: Optional[int],
                       year_to: Optional[int],
                       doctype: str = "AR") -> pd.DataFrame:
    ensure_scopus_init()

    base = build_base_clause(journal, issn)
    year_clause = build_year_clause(year_from, year_to)
    q = f'{base} {year_clause} AND DOCTYPE({doctype})'

    print("[query-1]", q)
    rows = scopus_search_safe(q)

    if not rows and (year_from is not None or year_to is not None):
        fallback_q = f'{base} AND DOCTYPE({doctype})'
        print("[query-2-fallback(no-year)]", fallback_q)
        rows = scopus_search_safe(fallback_q)

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df

    if (year_from is not None) or (year_to is not None):
        def _year_from_coverdate(x):
            try:
                return int(str(x)[:4])
            except Exception:
                return None
        df["__year"] = df.get("coverDate", pd.Series([None]*len(df))).map(_year_from_coverdate)
        if year_from is not None:
            df = df[df["__year"].fillna(-1) >= int(year_from)]
        if year_to is not None:
            df = df[df["__year"].fillna(9999) <= int(year_to)]
        df = df.drop(columns=["__year"], errors="ignore")

    if "eid" in df.columns:
        df = df.drop_duplicates("eid")
    elif "doi" in df.columns:
        df = df.drop_duplicates("doi")

    front = [
        "eid", "doi", "title", "coverDate", "publicationName", "creator",
        "citedby_count", "subtype", "subtypeDescription"
    ]
    ordered = [c for c in front if c in df.columns] + [c for c in df.columns if c not in front]
    df = df[ordered]
    return df

def main():
    ap = argparse.ArgumentParser(description="Collect Scopus records by Journal within a year range.")
    ap.add_argument("--journal", help='Journal title for SRCTITLE("...")')
    ap.add_argument("--issn", help="Journal ISSN (recommended for precision)")
    ap.add_argument("--year-from", type=int, required=True, help="Start year (inclusive)")
    ap.add_argument("--year-to", type=int, required=True, help="End year (inclusive)")
    ap.add_argument("--doctype", default="AR", help="Scopus DOCTYPE (default AR=Article)")
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    if not args.journal and not args.issn:
        raise SystemExit("Provide at least --journal or --issn")

    df = collect_by_journal(
        journal=args.journal,
        issn=args.issn,
        year_from=args.year_from,
        year_to=args.year_to,
        doctype=args.doctype
    )
    if df.empty:
        print("No records found.")
        pd.DataFrame().to_csv(args.out, index=False, encoding="utf-8")
        return
    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Saved: {args.out} | rows={len(df)}")

if __name__ == "__main__":
    main()
