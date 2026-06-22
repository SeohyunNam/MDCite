# IDCite
**Interdisciplinary Discovery Citation Dataset**

This repository provides the dataset construction pipeline used to build
**IDCite**, a large-scale multidisciplinary citation-event resource for
interdisciplinary discovery and scientific discovery research. IDCite models
scholarly interactions at the citation-event level rather than as
document-level citation links, and integrates citation events, citation
contexts, semantic citation intent annotations, normalized scholarly entities,
geographic information, and an ontology-ready knowledge graph spanning 21
Essential Science Indicators (ESI) fields.

IDCite is a graph-oriented resource derived from the previously released
**MDCite** citation dataset: MDCite provides large-scale citation-event records
and citation metadata, while IDCite extends this foundation through metadata
normalization, entity resolution, semantic enrichment, ontology construction,
and knowledge graph generation.

This repository accompanies the IDCite **Scientific Data** paper and contains
all code required to reproduce citation harvesting, metadata collection,
citation-event construction, semantic citation enrichment, entity resolution,
ontology-ready knowledge graph generation, and technical validation of the
released resource.

---

## Overview

- **Citation events:** 1,857,503
- **Citing papers:** 1,467,045
- **Seed (cited) papers:** 23,479
- **ESI fields:** 21
- **Representative WoS categories:** 21
- **Q1 journals:** 105
- **Citation intent labels:** 31 observed (30 valid; 7 canonical)
- **Knowledge graph:** 3,418,433 nodes / 6,855,117 edges
- **Data snapshot:** collected November 2025

IDCite preserves the scale, imbalance, and disciplinary heterogeneity of
real-world scholarly citations across the life sciences, medicine, engineering,
physical sciences, social sciences, humanities, and multidisciplinary domains.

---

## Code Structure

```
code/
├── Data Collection/
│   ├── collect_by_journal.py
│   ├── paper_title.py
│   └── batch_paper_title_multi.py
│
├── IDCite Construction/
│   └── ontology.py
│
└── Technical Validation/
    └── citationhub_technical_validation.py

README.md
```

---

## Data Sources

IDCite is constructed by integrating multiple large-scale scholarly data
sources:

### Scopus bibliographic records
Bibliographic metadata are collected via the **Scopus API** (using
`pybliometrics`), providing journal articles, citation counts, and rich
publication metadata. These records are used to identify influential seed
papers based on journal-stratified citation statistics.

### Web of Science (WoS) 2024 Subject Categories (JCR)
WoS subject categories are used to group journals by scientific field and to
select **Top-5 Q1 journals per representative category** (105 journals across
21 categories), enabling journal-stratified, field-aware sampling.

### OpenAlex API
The **OpenAlex API** is used for DOI resolution and large-scale citation-link
retrieval (i.e., identifying papers that cite the selected seed papers).

### Semantic Scholar Graph API
The **Semantic Scholar Graph API** is used to retrieve citation context spans
and citation intent signals associated with each citing paper. Citation
contexts correspond to textual spans surrounding in-text citation markers.

---

## Construction Pipeline

IDCite is built through a transparent and reproducible pipeline:

1. **Seed paper acquisition & journal-stratified sampling**
   - Journals grouped by WoS subject categories (21 categories × 5 journals)
   - Top 5% most-cited papers retained independently within each journal
   - Produces 23,479 multidisciplinary seed papers

2. **Bibliographic metadata collection**
   - Metadata harmonized across Scopus, OpenAlex, and Semantic Scholar

3. **Citation-event extraction**
   - Each citing publication is linked to a referenced seed paper as a directed
     citation event, preserving citation contexts and provenance

4. **Semantic citation intent annotation**
   - Citation events are annotated with citation intents using a graph neural
     network-based citation intent classification framework (weak supervision)

5. **Entity normalization & DOI processing**
   - Normalized identifiers for authors, affiliations, journals, cities,
     countries, fields, and citation intents

6. **Ontology-ready knowledge graph construction**
   - Heterogeneous nodes and typed edges (3,418,433 nodes / 6,855,117 edges)

7. **Release**
   - Structured Apache Parquet tables for citation events, publication
     metadata, normalized entities, and the knowledge graph

---

## Code Description

### Data Collection (`code/Data Collection/`)

#### `collect_by_journal.py`
- Uses the **Scopus API** to collect journal-level bibliographic metadata.
- Implements article collection per journal, citation-count retrieval, and
  journal-stratified Top-5% cited paper selection.
- Produces intermediate artifacts used to identify influential seed papers.

#### `paper_title.py`
- Core citation context extraction engine.
- Resolves DOIs (via OpenAlex if necessary) and retrieves citation links via
  the **OpenAlex API**.
- Retrieves citation context spans and intent signals via the
  **Semantic Scholar Graph API**.
- Outputs structured citation context records.

#### `batch_paper_title_multi.py`
- Batch execution wrapper for `paper_title.py`.
- Iterates over lists of influential papers for large-scale context extraction
  across multiple journal groups.

### IDCite Construction (`code/IDCite Construction/`)

#### `ontology.py`
- Builds the ontology-ready IDCite resource from the raw seed-paper and
  citing-context archives.
- Generates citation-event records, citing-paper and seed-paper tables,
  enriched citation events, and normalized entity lookup tables
  (authors, affiliations, journals, cities, countries, fields, intents).
- Constructs the ontology-ready knowledge graph (`kg_nodes.parquet`,
  `kg_edges.parquet`) with typed nodes and edges.
- Run with:
  ```bash
  python "code/IDCite Construction/ontology.py" --base-dir /path/to/wos_data
  ```
- Outputs are written to `<base-dir>/citationhub_v1_ontology_ready/`.

### Technical Validation (`code/Technical Validation/`)

#### `citationhub_technical_validation.py`
- Reproduces the technical-validation analyses reported in the paper:
  metadata completeness, citation-event referential integrity, citation-intent
  distribution and coverage, knowledge-graph integrity, and entity-table
  uniqueness.
- Writes per-check CSV tables and a bundled ZIP of all validation results.
- Run with:
  ```bash
  python "code/Technical Validation/citationhub_technical_validation.py" \
      --data-dir /path/to/wos_data/citationhub_v1_ontology_ready --make-figures
  ```

---

## Released Dataset Components

The released IDCite resource is distributed as structured Parquet files:

| Component | Rows | Columns |
|-----------|------|---------|
| citation_events.parquet | 1,857,503 | 20 |
| citation_events_enriched.parquet | 1,857,503 | 32 |
| citation_events_normalized.parquet | 1,857,503 | 23 |
| citing_papers.parquet | 1,467,045 | 7 |
| citing_papers_normalized.parquet | 1,467,045 | 8 |
| seed_cited_papers.parquet | 23,479 | 42 |
| seed_cited_papers_normalized.parquet | 23,479 | 48 |
| authors.parquet | 16,839 | 2 |
| affiliations.parquet | 5,271 | 2 |
| affiliation_geo.parquet | 5,352 | 6 |
| cities.parquet | 1,899 | 2 |
| countries.parquet | 108 | 2 |
| journals.parquet | 46,237 | 2 |
| fields.parquet | 21 | 3 |
| intents.parquet | 31 | 2 |
| kg_nodes.parquet | 3,418,433 | 14 |
| kg_edges.parquet | 6,855,117 | 3 |

---

## Knowledge Graph Schema

| Node Type | Attributes |
|-----------|------------|
| SeedPaper | doi, title, journal, author, affiliation, country, field, citedby_count |
| CitationEvent | event_id, citing_year, primary_intent, context, is_influential |
| CitingPaper | doi, title, year, venue |
| Intent | background, uses, similarities, motivation, differences, future_work, extends |
| Journal | journal_name |
| Author | author_id, author_name |
| Affiliation | affiliation_name |
| City | city_name |
| Country | country_name |
| Field | field_name |

| Edge Type | Relation |
|-----------|----------|
| HAS_CITING_PAPER | CitationEvent → CitingPaper |
| HAS_CITED_PAPER | CitationEvent → SeedPaper |
| HAS_PRIMARY_INTENT | CitationEvent → Intent |
| PUBLISHED_IN | SeedPaper → Journal |
| HAS_AUTHOR | SeedPaper → Author |
| HAS_AFFILIATION | SeedPaper → Affiliation |
| LOCATED_IN_CITY | Affiliation → City |
| LOCATED_IN_COUNTRY | Affiliation → Country |
| BELONGS_TO_FIELD | SeedPaper → Field |
| PUBLISHED_IN_VENUE | CitingPaper → Journal |

---

## Intended Use Cases

IDCite supports a wide range of research scenarios, including:

- Citation-aware information retrieval
- Intent-aware ranking and re-ranking
- Citation recommendation and candidate generation
- Large-scale citation intent classification
- Scientometric and bibliometric analysis
- Interdisciplinary knowledge discovery
- Knowledge graph analytics and link prediction
- AI-assisted scientific discovery workflows

---

## Reproducibility

### Requirements
- Python 3.9+
- `pandas`, `pyarrow`, `matplotlib` (figures only), and `pybliometrics`
  (Scopus collection only)

### API Requirements
Reproducing the full pipeline requires:
- Access to the **Scopus API** (institutional entitlement may be required)
- Access to the **OpenAlex API** (publicly available)
- Access to the **Semantic Scholar Graph API** (publicly available; rate limits apply)

API keys, where required, must be supplied via environment variables and are
not included in this repository. Because the underlying scholarly
infrastructures are continuously updated, IDCite should be interpreted as a
snapshot of the scholarly ecosystem corresponding to the November 2025
collection period.

---

## Code Availability

The software resources associated with IDCite are provided through two
complementary repositories:

- **Dataset construction pipeline (MDCite, this repository):**
  https://github.com/kecau/MDCite
- **Interactive dashboard and visualization platform (CitationHub):**
  https://github.com/kecau/CitationHub

---

## Data Availability

- **Original MDCite dataset (Zenodo):** https://zenodo.org/records/18536895
- **IDCite processed dataset and graph database (Hugging Face):**
  https://huggingface.co/datasets/Daniel0315/CitationHub
