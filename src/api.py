from fastapi import FastAPI, HTTPException, Form, Header, Depends
import asyncpg
import os
from jose import JWTError, jwt
from datetime import datetime, timedelta

# ==============================
# DATABASE CONFIG
# ==============================
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "herv")
DB_USER = os.getenv("DB_USER", "herv")
DB_PASS = os.getenv("DB_PASSWORD", "hervpass")

app = FastAPI(title="HERV Database API")

# ==============================
# SIMPLE AUTH CONFIG
# ==============================
SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_THIS_SECRET")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "herv_private")

# ==============================
# OPTIONAL TOKEN PARSER
# ==============================
async def optional_token(authorization: str | None = Header(None)):
    if authorization:
        scheme, _, param = authorization.partition(" ")
        if scheme.lower() == "bearer":
            return param
    return None

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# ==============================
# STARTUP / SHUTDOWN
# ==============================
@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME
    )

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

# ==============================
# LOGIN ENDPOINT (form-data)
# ==============================
@app.post("/token")
async def login(username: str = Form(...), password: str = Form(...)):
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Incorrect credentials")
    access_token = create_access_token(
        data={"sub": username, "role": "private"},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

# ==============================
# PUBLIC / PRIVATE ENDPOINTS
# ==============================
@app.get("/families")
async def get_families():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM herv_family ORDER BY name")
        return [dict(row) for row in rows]

from typing import Optional
@app.get("/primers")
async def get_primers(
    family: str = None,
    subgroup: str = None,
    component: str = None,
    dna: bool = None,
    token: str = Depends(optional_token)
):
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    # Query grouped by forward primer
    query = """
        SELECT pf.sequence AS forward_seq,
               pp.pair_index,
               pp.dna,
               f.name AS family_name,
               s.name AS subgroup_name,
               c.name AS component_name,
               json_agg(DISTINCT pr.sequence) AS reverse_seqs,
               COALESCE(
                 json_agg(
                   DISTINCT jsonb_build_object(
                       'title', b.title,
                       'doi', b.doi,
                       'year', b.year
                   )
                 ) FILTER (WHERE b.id IS NOT NULL), '[]'
               ) AS references
        FROM primer pf
        JOIN primer_pair pp ON pp.id = pf.primer_pair_id
        JOIN primer pr ON pr.primer_pair_id = pp.id AND pr.direction='reverse'
        JOIN primer_target pt ON pt.primer_pair_id = pp.id
        JOIN biological_target bt ON pt.biological_target_id = bt.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        LEFT JOIN primer_target_reference ptr ON ptr.primer_pair_id = pp.id AND ptr.biological_target_id = bt.id
        LEFT JOIN bibliography b ON b.id = ptr.bibliography_id
        WHERE pf.direction='forward'
    """

    params = []
    idx = 1
    if family:
        query += f" AND f.name = ${idx}"
        params.append(family)
        idx += 1
    if subgroup:
        query += f" AND s.name = ${idx}"
        params.append(subgroup)
        idx += 1
    if component:
        query += f" AND c.name = ${idx}"
        params.append(component)
        idx += 1
    if dna is not None:
        query += f" AND pp.dna = ${idx}"
        params.append(dna)

    query += " GROUP BY pf.sequence, pp.pair_index, pp.dna, f.name, s.name, c.name"

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    results = []
    for row in rows:
        item = dict(row)
        # Convert reverse_seqs from JSON string to list if needed
        if isinstance(item["reverse_seqs"], str):
            import json
            item["reverse_seqs"] = json.loads(item["reverse_seqs"])
        # Convert references from string to list
        if isinstance(item["references"], str):
            import json
            item["references"] = json.loads(item["references"])
        if not is_private:
            item.pop("hervolution", None)
        results.append(item)

    return results


@app.get("/primer_pairs")
async def search_primer_pairs(
    erv_family: Optional[str] = None,
    erv_subgroup: Optional[str] = None,
    erv_component: Optional[str] = None,
    herv_name: Optional[str] = None,
    dna: Optional[bool] = None,
    hervolution: Optional[bool] = None,
    token: str = Depends(optional_token)
):
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    query = """
        SELECT pf.sequence AS forward_seq,
               pp.pair_index,
               pp.dna,
               pp.hervolution,
               f.name AS family_name,
               s.name AS subgroup_name,
               c.name AS component_name,
               bt.name AS herv_name,
               json_agg(DISTINCT pr.sequence) AS reverse_seqs,
               COALESCE(
                 json_agg(
                   DISTINCT jsonb_build_object(
                       'title', b.title,
                       'doi', b.doi,
                       'year', b.year
                   )
                 ) FILTER (WHERE b.id IS NOT NULL), '[]'
               ) AS references
        FROM primer pf
        JOIN primer_pair pp ON pp.id = pf.primer_pair_id
        JOIN primer pr ON pr.primer_pair_id = pp.id AND pr.direction='reverse'
        JOIN primer_target pt ON pt.primer_pair_id = pp.id
        JOIN biological_target bt ON pt.biological_target_id = bt.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        LEFT JOIN primer_target_reference ptr ON ptr.primer_pair_id = pp.id AND ptr.biological_target_id = bt.id
        LEFT JOIN bibliography b ON b.id = ptr.bibliography_id
        WHERE pf.direction='forward'
    """

    params: List = []
    idx = 1
    if erv_family:
        query += f" AND f.name = ${idx}"; params.append(erv_family); idx += 1
    if erv_subgroup:
        query += f" AND s.name = ${idx}"; params.append(erv_subgroup); idx += 1
    if erv_component:
        query += f" AND c.name = ${idx}"; params.append(erv_component); idx += 1
    if herv_name:
        query += f" AND bt.name = ${idx}"; params.append(herv_name); idx += 1
    if dna is not None:
        query += f" AND pp.dna = ${idx}"; params.append(dna); idx += 1
    if hervolution is not None:
        query += f" AND pp.hervolution = ${idx}"; params.append(hervolution)

    query += " GROUP BY pf.sequence, pp.pair_index, pp.dna, pp.hervolution, f.name, s.name, c.name, bt.name"

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    results = []
    for row in rows:
        item = dict(row)
        # Ensure JSON fields are Python lists
        import json
        item["reverse_seqs"] = json.loads(item["reverse_seqs"]) if isinstance(item["reverse_seqs"], str) else item["reverse_seqs"]
        item["references"] = json.loads(item["references"]) if isinstance(item["references"], str) else item["references"]
        if not is_private:
            item.pop("hervolution", None)
        results.append(item)

    return results

@app.get("/primers_forward")
async def get_primers_forward(forward_seq: str):
    query = """
        SELECT pf.sequence AS forward_seq,
               json_agg(DISTINCT pr.sequence) AS reverse_seqs
        FROM primer pf
        JOIN primer_pair pp ON pp.id = pf.primer_pair_id
        JOIN primer pr ON pr.primer_pair_id = pp.id AND pr.direction='reverse'
        WHERE pf.direction='forward' AND pf.sequence = $1
        GROUP BY pf.sequence
    """
    async with app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, forward_seq)
        if not row:
            raise HTTPException(status_code=404, detail="Forward primer not found")
    # Convert jsonb to native Python list
    row_dict = dict(row)
    if isinstance(row_dict["reverse_seqs"], str):
        import json
        row_dict["reverse_seqs"] = json.loads(row_dict["reverse_seqs"])
    return row_dict


@app.get("/loci")
async def get_loci(pair_index: int = None, genome_build: str = None, name: str = None):
    query = """
        SELECT l.name,
               l.genbank_accession,
               lc.genome_build,
               lc.chromosome,
               lc.start_pos,
               lc.end_pos,
               lc.strand
        FROM locus l
        JOIN locus_coordinate lc ON lc.locus_id = l.id
        LEFT JOIN primer_locus pl ON pl.locus_id = l.id
        LEFT JOIN primer_pair pp ON pp.id = pl.primer_pair_id
        WHERE 1=1
    """

    params = []
    idx = 1

    if pair_index:
        query += f" AND pp.pair_index = ${idx}"
        params.append(pair_index)
        idx += 1
    if genome_build:
        query += f" AND lc.genome_build = ${idx}"
        params.append(genome_build)
        idx += 1
    if name:
        query += f" AND l.name = ${idx}"
        params.append(name)

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [dict(row) for row in rows]

@app.get("/primer_loci")
async def primer_loci(
    pair_index: Optional[int] = None,
    genome_build: Optional[str] = None,  # "hg19" or "hg38"
    herv_name: Optional[str] = None,
    token: str = Depends(optional_token)
):
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    query = """
        SELECT pf.sequence AS forward_seq,
               pp.pair_index,
               f.name AS family_name,
               s.name AS subgroup_name,
               c.name AS component_name,
               bt.name AS herv_name,
               lc.hg19_coord,
               lc.hg38_coord
        FROM primer pf
        JOIN primer_pair pp ON pp.id = pf.primer_pair_id
        JOIN primer_target pt ON pt.primer_pair_id = pp.id
        JOIN biological_target bt ON pt.biological_target_id = bt.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        JOIN locus l ON l.name = bt.name
        JOIN locus_coordinate lc ON lc.locus_id = l.id
        WHERE pf.direction='forward'
    """

    params = []
    idx = 1
    if pair_index:
        query += f" AND pp.pair_index = ${idx}"; params.append(pair_index); idx += 1
    if herv_name:
        query += f" AND bt.name = ${idx}"; params.append(herv_name); idx += 1
    if genome_build in ["hg19", "hg38"]:
        query += f" AND lc.{genome_build}_coord IS NOT NULL"

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [dict(row) for row in rows]


@app.get("/primer_stats")
async def primer_stats(token: str = Depends(optional_token)):
    """
    Aggregate statistics of primers per ERV family / subgroup / component.
    Returns counts of forward primers, reverse primers, and total primer pairs.
    """
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    query = """
        SELECT f.name AS family_name,
               s.name AS subgroup_name,
               c.name AS component_name,
               COUNT(DISTINCT pf.id) AS forward_count,
               COUNT(DISTINCT pr.id) AS reverse_count,
               COUNT(DISTINCT pp.id) AS primer_pair_count
        FROM primer pf
        JOIN primer_pair pp ON pp.id = pf.primer_pair_id
        JOIN primer pr ON pr.primer_pair_id = pp.id AND pr.direction='reverse'
        JOIN primer_target pt ON pt.primer_pair_id = pp.id
        JOIN biological_target bt ON pt.biological_target_id = bt.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        WHERE pf.direction='forward'
        GROUP BY f.name, s.name, c.name
        ORDER BY f.name, s.name, c.name
    """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query)

    return [dict(row) for row in rows]