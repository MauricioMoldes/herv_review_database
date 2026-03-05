from fastapi import FastAPI, HTTPException, Form, Header, Depends
import asyncpg
import os
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional, List
import json

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
# AUTH CONFIG
# ==============================
SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_THIS_SECRET")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "herv_private")

# ==============================
# AUTH HELPERS
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
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

# ==============================
# LOGIN
# ==============================
@app.post("/token")
async def login(username: str = Form(...), password: str = Form(...)):
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Incorrect credentials")

    access_token = create_access_token(
        data={"sub": username, "role": "private"},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    return {"access_token": access_token, "token_type": "bearer"}

# ==============================
# FAMILIES
# ==============================
@app.get("/families")
async def get_families():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name FROM herv_family ORDER BY name"
        )
    return [dict(row) for row in rows]

# ==============================
# PRIMER SET SEARCH
# ==============================
@app.get("/primer_sets")
async def get_primer_sets(
    family: Optional[str] = None,
    subgroup: Optional[str] = None,
    component: Optional[str] = None,
    herv_name: Optional[str] = None,
    dna: Optional[bool] = None,
    hervolution: Optional[bool] = None,
    token: str = Depends(optional_token),
):

    # ---- auth ----
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    # ---- query ----
    query = """
        SELECT
            ps.id,
            ps.set_index,
            ps.dna,
            ps.hervolution,

            f.name AS family_name,
            s.name AS subgroup_name,
            c.name AS component_name,
            (f.name || COALESCE('_' || s.name, '') || '_' || c.name) AS herv_name,

            json_agg(DISTINCT
                CASE WHEN p.direction='forward'
                THEN jsonb_build_object(
                    'name', p.name,
                    'sequence', p.sequence
                )
                END
            ) FILTER (WHERE p.direction='forward') AS forward_primers,

            json_agg(DISTINCT
                CASE WHEN p.direction='reverse'
                THEN jsonb_build_object(
                    'name', p.name,
                    'sequence', p.sequence
                )
                END
            ) FILTER (WHERE p.direction='reverse') AS reverse_primers,

            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'title', b.title,
                        'doi', b.doi,
                        'year', b.year
                    )
                ) FILTER (WHERE b.id IS NOT NULL),
                '[]'
            ) AS references

        FROM primer_set ps
        JOIN primer p ON p.primer_set_id = ps.id
        JOIN primer_set_target pst ON pst.primer_set_id = ps.id
        JOIN biological_target bt ON bt.id = pst.biological_target_id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        LEFT JOIN primer_set_target_reference pstr
            ON pstr.primer_set_id = ps.id
            AND pstr.biological_target_id = bt.id
        LEFT JOIN bibliography b ON b.id = pstr.bibliography_id
        WHERE 1=1
    """

    params: List = []
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

    if herv_name:
        query += f" AND (f.name || COALESCE('_' || s.name, '') || '_' || c.name) = ${idx}"
        params.append(herv_name)
        idx += 1

    if dna is not None:
        query += f" AND ps.dna = ${idx}"
        params.append(dna)
        idx += 1

    if hervolution is not None:
        query += f" AND ps.hervolution = ${idx}"
        params.append(hervolution)
        idx += 1

    query += """
        GROUP BY ps.id, ps.set_index, ps.dna, ps.hervolution,
                 f.name, s.name, c.name
        ORDER BY ps.set_index;
    """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    results = []
    for row in rows:
        item = dict(row)
        if isinstance(item["references"], str):
            item["references"] = json.loads(item["references"])
        if not is_private:
            item.pop("hervolution", None)
        results.append(item)

    return results

# ==============================
# LOCI
# ==============================
@app.get("/primer_loci")
async def primer_loci(
    set_index: Optional[int] = None,
    genome_build: Optional[str] = None,
):

    query = """
        SELECT
            ps.set_index,
            (f.name || COALESCE('_' || s.name, '') || '_' || c.name) AS herv_name,
            lc.genome_build,
            lc.chromosome,
            lc.start_pos,
            lc.end_pos,
            lc.strand

        FROM primer_set ps
        JOIN primer_set_target pst ON pst.primer_set_id = ps.id
        JOIN biological_target bt ON bt.id = pst.biological_target_id
        JOIN primer_set_locus psl ON psl.primer_set_id = ps.id
        JOIN locus l ON l.id = psl.locus_id
        JOIN locus_coordinate lc ON lc.locus_id = l.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        WHERE 1=1
    """

    params = []
    idx = 1

    if set_index:
        query += f" AND ps.set_index = ${idx}"
        params.append(set_index)
        idx += 1

    if genome_build:
        query += f" AND lc.genome_build = ${idx}"
        params.append(genome_build)

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [dict(row) for row in rows]

# ==============================
# STATS
# ==============================
@app.get("/primer_stats")
async def primer_stats():

    query = """
        SELECT
            f.name AS family_name,
            s.name AS subgroup_name,
            c.name AS component_name,

            COUNT(DISTINCT ps.id) AS primer_set_count,
            COUNT(DISTINCT p.id) FILTER (WHERE p.direction='forward') AS forward_count,
            COUNT(DISTINCT p.id) FILTER (WHERE p.direction='reverse') AS reverse_count

        FROM primer_set ps
        JOIN primer p ON p.primer_set_id = ps.id
        JOIN primer_set_target pst ON pst.primer_set_id = ps.id
        JOIN biological_target bt ON bt.id = pst.biological_target_id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id

        GROUP BY f.name, s.name, c.name
        ORDER BY f.name, s.name, c.name;
    """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query)

    return [dict(row) for row in rows]

# ==============================
# PRIMER PAIR LOOKUP WITH REFERENCES
# ==============================

def normalize_seq(seq: str) -> str:
    """Remove spaces and uppercase a primer sequence."""
    return seq.replace(" ", "").upper() if seq else seq

@app.get("/primers_forward")
async def primers_forward(forward_seq: str):
    """
    Given a forward primer sequence, return all reverse primers
    associated with the same biological targets, including references.
    """
    fwd_norm = normalize_seq(forward_seq)

    query = """
        SELECT
            ps.set_index,
            f.name AS family_name,
            COALESCE(s.name,'Unspecified') AS subgroup_name,
            c.name AS component_name,

            json_agg(DISTINCT
                CASE WHEN p_rev.direction='reverse'
                THEN jsonb_build_object(
                    'name', p_rev.name,
                    'sequence', p_rev.sequence
                )
                END
            ) FILTER (WHERE p_rev.direction='reverse') AS reverse_primers,

            COALESCE(
                json_agg(DISTINCT
                    jsonb_build_object(
                        'title', b.title,
                        'doi', b.doi,
                        'year', b.year
                    )
                ) FILTER (WHERE b.id IS NOT NULL),
                '[]'
            ) AS references

        FROM primer p_forward
        JOIN primer_set_target pst_fw ON pst_fw.primer_set_id = p_forward.primer_set_id
        JOIN biological_target bt ON bt.id = pst_fw.biological_target_id
        JOIN primer_set_target pst_rev ON pst_rev.biological_target_id = bt.id
        JOIN primer_set ps ON ps.id = pst_rev.primer_set_id
        JOIN primer p_rev ON p_rev.primer_set_id = ps.id AND p_rev.direction='reverse'
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        LEFT JOIN primer_set_target_reference pstr
            ON pstr.primer_set_id = ps.id
            AND pstr.biological_target_id = bt.id
        LEFT JOIN bibliography b ON b.id = pstr.bibliography_id
        WHERE p_forward.direction='forward'
          AND REPLACE(UPPER(p_forward.sequence),' ','') = $1
        GROUP BY ps.set_index, f.name, s.name, c.name
        ORDER BY ps.set_index;
    """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fwd_norm)

    results = []
    for row in rows:
        item = dict(row)
        # Convert JSONB to Python lists
        if isinstance(item["reverse_primers"], str):
            item["reverse_primers"] = json.loads(item["reverse_primers"])
        if isinstance(item["references"], str):
            item["references"] = json.loads(item["references"])
        results.append(item)

    return results


@app.get("/primers_reverse")
async def primers_reverse(reverse_seq: str):
    """
    Given a reverse primer sequence, return all forward primers
    associated with the same biological targets, including references.
    """
    rev_norm = normalize_seq(reverse_seq)

    query = """
        SELECT
            ps.set_index,
            f.name AS family_name,
            COALESCE(s.name,'Unspecified') AS subgroup_name,
            c.name AS component_name,

            json_agg(DISTINCT
                CASE WHEN p_fwd.direction='forward'
                THEN jsonb_build_object(
                    'name', p_fwd.name,
                    'sequence', p_fwd.sequence
                )
                END
            ) FILTER (WHERE p_fwd.direction='forward') AS forward_primers,

            COALESCE(
                json_agg(DISTINCT
                    jsonb_build_object(
                        'title', b.title,
                        'doi', b.doi,
                        'year', b.year
                    )
                ) FILTER (WHERE b.id IS NOT NULL),
                '[]'
            ) AS references

        FROM primer p_rev
        JOIN primer_set_target pst_rev ON pst_rev.primer_set_id = p_rev.primer_set_id
        JOIN biological_target bt ON bt.id = pst_rev.biological_target_id
        JOIN primer_set_target pst_fwd ON pst_fwd.biological_target_id = bt.id
        JOIN primer_set ps ON ps.id = pst_fwd.primer_set_id
        JOIN primer p_fwd ON p_fwd.primer_set_id = ps.id AND p_fwd.direction='forward'
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
        LEFT JOIN primer_set_target_reference pstr
            ON pstr.primer_set_id = ps.id
            AND pstr.biological_target_id = bt.id
        LEFT JOIN bibliography b ON b.id = pstr.bibliography_id
        WHERE p_rev.direction='reverse'
          AND REPLACE(UPPER(p_rev.sequence),' ','') = $1
        GROUP BY ps.set_index, f.name, s.name, c.name
        ORDER BY ps.set_index;
    """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, rev_norm)

    results = []
    for row in rows:
        item = dict(row)
        if isinstance(item["forward_primers"], str):
            item["forward_primers"] = json.loads(item["forward_primers"])
        if isinstance(item["references"], str):
            item["references"] = json.loads(item["references"])
        results.append(item)

    return results


@app.get("/primers")
async def get_primers(
    forward_seq: Optional[str] = None,
    reverse_seq: Optional[str] = None,
    family: Optional[str] = None,
    subgroup: Optional[str] = None,
    component: Optional[str] = None,
    herv_name: Optional[str] = None,
    dna: Optional[bool] = None,
    token: str = Depends(optional_token)
):
    # -------------------------------
    # Normalize input sequences
    # -------------------------------
    if forward_seq:
        forward_seq = forward_seq.replace(" ", "").upper()
    if reverse_seq:
        reverse_seq = reverse_seq.replace(" ", "").upper()

    # -------------------------------
    # Auth check
    # -------------------------------
    is_private = False
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if payload.get("role") == "private":
                is_private = True
        except JWTError:
            pass

    # -------------------------------
    # Base query
    # -------------------------------
    query = """
    SELECT
        ps.set_index,
        ps.dna,
        f.name AS family_name,
        COALESCE(s.name,'Unspecified') AS subgroup_name,
        c.name AS component_name,
        json_agg(DISTINCT jsonb_build_object(
            'name', p_forward.name,
            'sequence', p_forward.sequence
        )) FILTER (WHERE p_forward.id IS NOT NULL) AS forward_primers,
        json_agg(DISTINCT jsonb_build_object(
            'name', p_rev.name,
            'sequence', p_rev.sequence
        )) FILTER (WHERE p_rev.id IS NOT NULL) AS reverse_primers,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'title', b.title,
                    'doi', b.doi,
                    'year', b.year
                )
            ) FILTER (WHERE b.id IS NOT NULL),
            '[]'
        ) AS references
    FROM primer p_forward
    JOIN primer_set_target pst_fw ON pst_fw.primer_set_id = p_forward.primer_set_id
    JOIN biological_target bt ON bt.id = pst_fw.biological_target_id
    JOIN primer_set_target pst_rev ON pst_rev.biological_target_id = bt.id
    JOIN primer_set ps ON ps.id = pst_rev.primer_set_id
    JOIN primer p_rev ON p_rev.primer_set_id = ps.id AND p_rev.direction='reverse'
    JOIN herv_family f ON bt.herv_family_id = f.id
    LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
    JOIN herv_component c ON bt.herv_component_id = c.id
    LEFT JOIN primer_set_target_reference pstr
        ON pstr.primer_set_id = ps.id
        AND pstr.biological_target_id = bt.id
    LEFT JOIN bibliography b
        ON b.id = pstr.bibliography_id
    WHERE p_forward.direction='forward'
    """

    # -------------------------------
    # Dynamic filters
    # -------------------------------
    params: List = []
    idx = 1

    if forward_seq:
        query += f" AND UPPER(REPLACE(p_forward.sequence,' ','')) = ${idx}"
        params.append(forward_seq)
        idx += 1

    if reverse_seq:
        query += f" AND UPPER(REPLACE(p_rev.sequence,' ','')) = ${idx}"
        params.append(reverse_seq)
        idx += 1

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

    if herv_name:
        query += f" AND (f.name || COALESCE('_' || s.name,'') || '_' || c.name) = ${idx}"
        params.append(herv_name)
        idx += 1

    if dna is not None:
        query += f" AND ps.dna = ${idx}"
        params.append(dna)
        idx += 1

    query += """
    GROUP BY ps.set_index, ps.dna, f.name, s.name, c.name
    ORDER BY ps.set_index;
    """

    # -------------------------------
    # Execute query
    # -------------------------------
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    # -------------------------------
    # Process results
    # -------------------------------
    results = []
    for row in rows:
        item = dict(row)

        # Ensure empty arrays instead of null
        item["forward_primers"] = item.get("forward_primers") or []
        item["reverse_primers"] = item.get("reverse_primers") or []
        item["references"] = item.get("references") or []

        # Hide private fields if no token
        if not is_private:
            item.pop("hervolution", None)

        results.append(item)

    return results