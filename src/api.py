from fastapi import FastAPI, HTTPException
import asyncpg
import os

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "herv")
DB_USER = os.getenv("DB_USER", "herv")
DB_PASS = os.getenv("DB_PASSWORD", "hervpass")

app = FastAPI(title="HERV Database API")

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


@app.get("/families")
async def get_families():
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM herv_family ORDER BY name")
        return [dict(row) for row in rows]


@app.get("/primers")
async def get_primers(family_id: int = None):
    query = """
        SELECT pp.id AS primer_pair_id, pp.pair_index, pp.dna, pp.hervolution,
               f.name AS family_name,
               s.name AS subgroup_name,
               c.name AS component_name
        FROM primer_pair pp
        JOIN primer_target pt ON pp.id = pt.primer_pair_id
        JOIN biological_target bt ON pt.biological_target_id = bt.id
        JOIN herv_family f ON bt.herv_family_id = f.id
        LEFT JOIN herv_subgroup s ON bt.herv_subgroup_id = s.id
        JOIN herv_component c ON bt.herv_component_id = c.id
    """
    params = []
    if family_id:
        query += " WHERE f.id = $1"
        params.append(family_id)

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@app.get("/loci")
async def get_loci(genome_build: str = None):
    query = "SELECT * FROM locus_coordinate"
    params = []
    if genome_build:
        query += " WHERE genome_build = $1"
        params.append(genome_build)

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]

