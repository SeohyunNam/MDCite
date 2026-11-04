# Pipeline 5 — IDCite v4 (OpenAlex Metadata Replacement)

Builds the **IDCite v4** release from the v3 tables. v4 removes all
Scopus-derived content: every seed-paper attribute that v3 took from Scopus is
replaced with **OpenAlex** (CC0) metadata retrieved by DOI, and the normalized
entity tables and the knowledge graph are rebuilt with the same logic as
[`../4_IDCite_Construction/ontology.py`](../4_IDCite_Construction/ontology.py).

Citation events, citation contexts, citing papers, and citation intents are
unchanged from v3.

## Flow

```
v3 seed_cited_papers.parquet (DOIs)
        │  fetch_openalex_seed.py
        │  OpenAlex works (batched by DOI, single-DOI retry) + institutions
        ▼
OpenAlex cache (works.jsonl, institutions.jsonl)
        │  build_idcite_v4.py  (+ v3 tables)
        │  seed papers from OpenAlex, enriched events re-joined,
        │  entity tables and knowledge graph rebuilt
        ▼
17 IDCite v4 Parquet tables
        │  compare_v3_v4.py
        ▼
v3 → v4 comparison and Scopus-field check
```

## What changes from v3

| Table | v4 |
| --- | --- |
| `seed_cited_papers`, `seed_cited_papers_normalized` | Rebuilt from OpenAlex; all Scopus fields removed |
| `citation_events_enriched` | `cited_*` columns re-joined from the OpenAlex seed table |
| `authors`, `affiliations`, `affiliation_geo`, `cities`, `countries`, `journals` | Rebuilt from the OpenAlex seed metadata |
| `citing_papers_normalized` | `journal_id` re-mapped to the rebuilt journal table |
| `kg_nodes`, `kg_edges` | Rebuilt |
| `citation_events`, `citation_events_normalized`, `citing_papers`, `fields`, `intents` | Unchanged (copied from v3) |

**Removed seed-paper fields (Scopus):** `eid`, `afid`, `source_id`, `pii`,
`subtype`, `subtype_description`, `cover_date`, `cover_display_date`,
`author_names`, `author_ids`, `author_afids`, `authkeywords`, `description`,
`openaccess`, `freetoread`, `freetoread_label`, `fund_acr`, `fund_no`,
`fund_sponsor`, `issn`, `eissn`, `aggregation_type`, `volume`,
`issue_identifier`, `article_number`, `page_range`, `pubmed_id`, `source_file`.

**Seed-paper fields from OpenAlex:** `openalex_id`, `title`,
`publication_date`, `publication_year`, `publication_name`,
`source_openalex_id`, `issn_l`, `creator` (first author),
`creator_openalex_id`, `author_count`, `affilname` (first listed institution),
`affiliation_openalex_id`, `affiliation_ror`, `affiliation_city`,
`affiliation_country`, `citedby_count` (OpenAlex `cited_by_count`), `type`,
`volume`, `issue`, `first_page`, `last_page`, `pmid`, `is_oa`, `oa_status`,
`funders`, `metadata_source`.

**Kept from v3 (not Scopus):** `seed_paper_id`, `doi`, `group` (ESI field),
`category` (WoS category name), `journal_clarivate` (journal name),
`is_seed_top5pct`.

The seed set itself (23,479 DOIs) was selected with Scopus citation counts in
Pipeline 1 and is unchanged; only the DOIs are redistributed. Seed citation
counts in v4 are OpenAlex counts at retrieval time and may differ from the
Scopus counts used for selection.

## v3 → v4 numbers

| Item | v3 | v4 |
| --- | --- | --- |
| Seed papers with OpenAlex metadata | — | 23,252 / 23,479 (99.03%) |
| `authors.parquet` | 16,839 | 19,865 |
| `affiliations.parquet` | 5,271 | 4,074 |
| `affiliation_geo.parquet` | 5,352 | 4,088 |
| `cities.parquet` | 1,899 | 1,760 |
| `countries.parquet` | 108 | 120 |
| `journals.parquet` | 46,237 | 46,234 |
| `kg_nodes.parquet` | 3,418,433 | 3,420,132 |
| `kg_edges.parquet` | 6,855,117 | 6,851,623 |
| `seed_cited_papers` columns | 42 | 32 |
| `seed_cited_papers_normalized` columns | 48 | 38 |

The 227 seed papers not indexed in OpenAlex are retained with their DOI and
sampling labels only. Citation events (1,857,503), citing papers (1,467,045),
intent labels (31), fields (21), and the seed-paper linkage of citation events
(92.11%) are unchanged.

## Usage

```bash
python fetch_openalex_seed.py \
    --seed-parquet /path/to/citationhub_v1_ontology_ready/seed_cited_papers.parquet \
    --cache-dir /path/to/openalex_cache

python build_idcite_v4.py \
    --v3-dir /path/to/citationhub_v1_ontology_ready \
    --cache-dir /path/to/openalex_cache \
    --out-dir /path/to/IDCite_v4

python compare_v3_v4.py \
    --v3-dir /path/to/citationhub_v1_ontology_ready \
    --v4-dir /path/to/IDCite_v4
```

The OpenAlex API is used without credentials; anonymous requests share a small
daily budget, so an OpenAlex API key may be needed for repeated runs. The v4
seed metadata were retrieved in November 2026. The technical-validation script
in [`../Technical_Validation/`](../Technical_Validation/) runs unchanged on the
v4 tables (`--data-dir /path/to/IDCite_v4`). Dependencies: see
[`../requirements.txt`](../requirements.txt).
