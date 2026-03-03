# herv_review_database


# HERV Primer Database API

This repository hosts a **PostgreSQL database** and **FastAPI service** for the curated **Human Endogenous Retrovirus (HERV) primer dataset**. It includes:

- Primer pairs  
- Biological targets  
- Genomic loci  
- References from literature  

The API allows querying:

- Primers  
- HERV families, subgroups, and components  
- Associated references (DOI, title, year)


## Features

- Store and query **HERV families, subgroups, and components**.
- Store and query **primer pairs** with forward/reverse sequences.
- Link primers to **biological targets** and **genomic loci**.
- Include **literature references** for primers and targets (DOI, title, year).
- Support **DNA/RNA-specific primers** filtering.
- FastAPI provides **RESTful endpoints** for data retrieval.
- Optional **token-based access** for private endpoints.


## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.11 (for local development, optional)
- `curl` or Postman for testing API endpoints

### Clone the repository

```bash
git clone https://github.com/yourusername/herv-review-database.git
cd herv-review-database
```

## Example API Responses

### GET /primers

```json
[
  {
    "pair_index": 80,
    "dna": false,
    "family_name": "HERV-E",
    "subgroup_name": "Unspecified",
    "component_name": "gag",
    "forward_seq": "CAT CAA CCT ACT TGG GAT GAT TGT CAR CA",
    "reverse_seq": "CAA TGA CCT TTT TCT TTA CAG TAG GCR CA",
    "references": [
      {
        "doi": "10.1099/0022-1317-73-9-2463",
        "year": 1992,
        "title": "Expression of human endogenous retroviral sequences in peripheral blood mononuclear cells of healthy individuals"
      },
      {
        "doi": "10.1155/2015/164529",
        "year": 2015,
        "title": "Transcriptional Activity of Human Endogenous Retroviruses in Human Peripheral Blood Mononuclear Cells"
      }
    ]
  },
  {
    "pair_index": 81,
    "dna": false,
    "family_name": "HERV-E",
    "subgroup_name": "Unspecified",
    "component_name": "gag",
    "forward_seq": "AAC CCC ACT TGG GCT GAT TGC CAC CA",
    "reverse_seq": "CAA TGA CCT TTT TCT TTA CAG TAG GCR CA",
    "references": [
      {
        "doi": "10.1099/0022-1317-73-9-2463",
        "year": 1992,
        "title": "Expression of human endogenous retroviral sequences in peripheral blood mononuclear cells of healthy individuals"
      }
    ]
  }
]

## Authentication

Some endpoints support optional authentication using **Bearer tokens**. A token allows access to private data (e.g., the `hervolution` column).  

### Obtain a Token

Send a POST request with form data (`username` and `password`) to `/token`:

```bash
curl -X POST "http://localhost:8001/token" \
  -F "username=admin" \
  -F "password=herv_private"
```



## License

This project is licensed under the **MIT License**. See the `LICENSE` file for full details.



