# MDCite  
**A Large-Scale Multi-Disciplinary Citation Context Dataset**

MDCite is a large-scale, multi-disciplinary citation context dataset designed to support research in citation-aware scholarly information retrieval (IR).  
The dataset treats citation contexts local textual spans surrounding in-text citations as the primary unit of retrieval and analysis, enabling fine-grained evaluation of intent-aware retrieval, ranking, and candidate generation methods.

This repository accompanies the **SIGIR 2026 Resource Paper** introducing MDCite and provides all artifacts required to reproduce the dataset construction pipeline and baseline retrieval experiments.

---

## Overview

- **Citation contexts:** 2,057,196  
- **Citing papers:** ~100,000  
- **Scientific fields:** 21  
- **Publication venues:** >1,000  
- **Citation intent classes:** 7  
- **Time span:** 2000–2025  

MDCite is designed as a **realistic IR test collection**, preserving the scale, imbalance, and disciplinary heterogeneity of real-world scholarly citations.

---

## Code Structure

```
├── Code/Dataset Construction/
│   ├── collect_by_journal.py
│   ├── paper_title.py
│   └── batch_paper_title_multi.py
│
├── Code/Evaluation/
│   └── MDCite_SciCite_eval.ipynb
│
└── README.md
```

---

## Data Sources

MDCite is constructed by integrating multiple large-scale scholarly data sources:

### Scopus bibliographic records (2000–2024)
Bibliographic metadata are collected via the **Scopus API** (using `pybliometrics`), providing access to journal articles, citation counts, and rich publication metadata.  
These records form the foundation of the dataset and are used to identify influential papers based on citation statistics.

### Web of Science (WoS) 2024 Subject Categories
WoS subject categories are used to group journals by scientific field and to select **Top-5 Q1 journals per field**, enabling journal-stratified and field-aware normalization.

### OpenAlex API
The **OpenAlex API** is used for DOI resolution and large-scale citation link retrieval (i.e., identifying papers that cite selected influential works).

### Semantic Scholar Graph API

The **Semantic Scholar Graph API** is used to retrieve citation context spans and citation intent labels associated with each citing paper.  
Citation contexts correspond to textual spans surrounding in-text citation markers.

---

## Dataset Construction Pipeline

The MDCite dataset is built through a transparent and reproducible pipeline:

1. **Journal selection**
   - Journals grouped by WoS subject categories  
   - Top-5 Q1 journals selected per scientific field  

2. **Citation-based filtering**
   - Top 5% most-cited papers identified independently within each journal  
   - Prevents dominance by citation-intensive fields  

3. **Citation context extraction**
   - Citation contexts extracted from papers citing the selected influential papers  
   - Each context corresponds to a textual span surrounding an in-text citation marker  

4. **Citation intent classification**
   - Each context is automatically assigned one of seven intents:  
     `background`, `uses`, `similarities`, `differences`, `motivation`, `extends`, `future_work`

5. **Dataset variants**
   - Multi-intent intermediate artifacts (for provenance)  
   - Single-intent benchmark variant (for standard IR and classification)

---

## Code Description

### Data Construction (`Code/Dataset Construction/`)

#### `collect_by_journal.py`
- Uses the **Scopus API** to collect journal-level bibliographic metadata.
- Implements:
  - Article collection per journal  
  - Citation count retrieval  
  - Journal-stratified Top-5% cited paper selection  
- Produces intermediate artifacts used for identifying influential papers.

#### `paper_title.py`
- Core citation context extraction engine.
- Resolves DOIs (via OpenAlex if necessary).
- Retrieves citation links via the **OpenAlex API**.
- Retrieves citation context spans and intent labels via the **Semantic Scholar Graph API**.
- Outputs structured citation context records used to construct the MDCite dataset.
  
#### `batch_paper_title_multi.py`
- Batch execution wrapper for paper_title.py.
- Iterates over lists of influential papers and performs large-scale context extraction.
- Supports scalable querying across multiple journal groups.

Together, these scripts implement the **dataset construction pipeline** described in the SIGIR 2026 paper.

---

### Evaluation (`Code/Evaluation/`)

#### `MDCite_SciCite_eval.ipynb`
- Implements **BM25-based citation context retrieval**.
- Evaluates retrieval performance on:
  - **MDCite (single-intent variant)**  
  - **SciCite** (for comparison)
- Reports standard IR metrics:
  - **nDCG@10**
  - **Recall@k** (e.g., Recall@100, Recall@1000)
- Supports:
  - Intent-wise evaluation  
  - Dataset-level comparison  
  - Deep-retrieval analysis under large relevance sets  

This code demonstrates that MDCite naturally supports candidate generation and deep-retrieval evaluation, where recall improves only at larger cutoffs due to large and distributed relevance sets.

---

## Dataset Schema

Each citation context instance contains the following fields:

| Field | Description |
|------|------------|
| `text` | Citation context text |
| `label` | Functional citation intent |
| `field` | Scientific field |
| `group_id` | Citing paper identifier |
| `paperId` | Semantic scholar work identifier |
| `doi` | Digital Object Identifier |
| `venue` | Publication venue |
| `year` | Publication year |
| `source_file` | Provenance identifier |

The `group_id` field enables document-aware evaluation and prevents information leakage in retrieval experiments.

---

## Intended Use Cases

MDCite supports a wide range of research scenarios, including:

- Citation-aware information retrieval  
- Intent-aware ranking and re-ranking  
- Candidate generation analysis  
- Large-scale citation intent classification  
- Scholarly search and citation analysis  

Baseline BM25 experiments show that MDCite naturally supports deep-retrieval evaluation, where recall improves only at larger cutoffs.

---

## Reproducibility

### API Requirements

Reproducing the full dataset construction requires:

- Access to the **Scopus API** (institutional entitlement may be required)
- Access to the **OpenAlex API** (publicly available)
- Access to the **Semantic Scholar Graph API** (publicly available; rate limits apply)

API keys, where required, must be supplied via environment variables and are not included in this repository.
All scripts, intermediate artifacts, and processed datasets are released to support end-to-end reproducible research.  
Document-level identifiers and clearly separated construction and evaluation code help avoid information leakage and ensure consistent experimental setups.

---

## Data Availability

The processed citation context and intent datasets used in this study are
publicly available via Zenodo at https://doi.org/10.5281/zenodo.18410050.

---


