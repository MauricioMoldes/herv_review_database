import os
import re
import pandas as pd
import psycopg2
import bibtexparser

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "hervdb")
DB_USER = os.getenv("DB_USER", "herv")
DB_PASS = os.getenv("DB_PASSWORD", "hervpass")

DATA_PATH = os.path.join("/app/data", "primERV.tsv")
TEX_PATH = os.path.join("/app/data", "primers.tex")
BIB_PATH = os.path.join("/app/data", "bibliography.bib")

DB_URL = f"dbname={DB_NAME} user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT}"

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def parse_coord(coord_string):
    """Parse coordinates like chr7:12345-12500(+)"""
    if not coord_string or pd.isna(coord_string):
        return None
    coord_string = str(coord_string).strip()
    match = re.match(r"(chr?\w+):(\d+)-(\d+)\(?([+-])?\)?", coord_string)
    if not match:
        return None
    chrom, start, end, strand = match.groups()
    return chrom, int(start), int(end), strand

def get_or_create(cur, table, unique_col, value):
    """Insert value if not exists, return id"""
    if isinstance(value, float) and pd.isna(value):
        value = None
    if value in (None, ""):
        return None
    cur.execute(
        f"""
        INSERT INTO {table} ({unique_col})
        VALUES (%s)
        ON CONFLICT ({unique_col}) DO NOTHING
        RETURNING id;
        """,
        (value,)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    cur.execute(f"SELECT id FROM {table} WHERE {unique_col} = %s", (value,))
    return cur.fetchone()[0]

def parse_tex_citations(tex_path):
    """Parse primer_pair indices to citation keys from primers.tex"""
    citations = {}
    with open(tex_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract lines like "43--48 &~\cite{Smith2020,Doe2019} \\"
    matches = re.findall(r"([\d,\\-]+)\s*&~\\cite\{([^\}]+)\}", content)
    for idx_str, keys in matches:
        # Handle ranges like 43--48 and comma-separated lists
        key_list = [k.strip() for k in keys.split(",")]
        idx_items = []
        for part in idx_str.split(","):
            if "--" in part:
                start, end = map(int, part.split("--"))
                idx_items.extend(range(start, end + 1))
            else:
                idx_items.append(int(part))
        for idx in idx_items:
            citations.setdefault(idx, set()).update(key_list)
    return citations

def load_bib_file(bib_path):
    """Return dict of citation_key -> bib entry dict"""
    with open(bib_path, encoding="utf-8") as bibtex_file:
        bib_database = bibtexparser.load(bibtex_file)
    bib_dict = {}
    for entry in bib_database.entries:
        bib_dict[entry.get("ID")] = entry
    return bib_dict

# ---------------------------------------------------------
# Main Loader
# ---------------------------------------------------------

def main():
    print(f"Loading TSV from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH, sep="\t")

    # Parse LaTeX citations
    tex_citations = parse_tex_citations(TEX_PATH)
    # Parse BibTeX
    bib_entries = load_bib_file(BIB_PATH)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # ---------------------------------
    # Forward primer buffer
    # ---------------------------------
    current_forward_name = None
    current_forward_seq = None

    for _, row in df.iterrows():
        # -----------------------------
        # HERV Family
        # -----------------------------
        family_id = get_or_create(cur, "herv_family", "name", row.get("ERV_group"))

        # -----------------------------
        # Subgroup
        # -----------------------------
        subgroup_id = None
        subgroup_value = row.get("ERV_subgroup")
        if pd.notna(subgroup_value):
            cur.execute("""
                INSERT INTO herv_subgroup (herv_family_id, name)
                VALUES (%s, %s)
                ON CONFLICT (herv_family_id, name) DO NOTHING
                RETURNING id
            """, (family_id, subgroup_value))
            res = cur.fetchone()
            if res:
                subgroup_id = res[0]
            else:
                cur.execute("SELECT id FROM herv_subgroup WHERE herv_family_id=%s AND name=%s",
                            (family_id, subgroup_value))
                subgroup_id = cur.fetchone()[0]

        # -----------------------------
        # Component
        # -----------------------------
        component_id = get_or_create(cur, "herv_component", "name", row.get("ERV_component"))

        # -----------------------------
        # Biological Target
        # -----------------------------
        target_id = None
        if family_id and component_id:
            cur.execute("""
                INSERT INTO biological_target (herv_family_id, herv_subgroup_id, herv_component_id)
                VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id
            """, (family_id, subgroup_id, component_id))
            res = cur.fetchone()
            if res:
                target_id = res[0]
            else:
                cur.execute("""
                    SELECT id FROM biological_target
                    WHERE herv_family_id=%s AND herv_subgroup_id IS NOT DISTINCT FROM %s
                    AND herv_component_id=%s
                """, (family_id, subgroup_id, component_id))
                target_id = cur.fetchone()[0]

        # -----------------------------
        # Primer Pair
        # -----------------------------
        cur.execute("""
            INSERT INTO primer_pair (pair_index, dna, hervolution)
            VALUES (%s,%s,%s) ON CONFLICT (pair_index) DO NOTHING RETURNING id
        """, (row["primer_pair_idx"], bool(row["DNA"]), bool(row["HERVolution"])))
        res = cur.fetchone()
        if res:
            primer_pair_id = res[0]
        else:
            cur.execute("SELECT id FROM primer_pair WHERE pair_index=%s", (row["primer_pair_idx"],))
            primer_pair_id = cur.fetchone()[0]

        # -----------------------------
        # Forward / Reverse Primers (state-aware)
        # -----------------------------
        fw_name = row.get("fw_name")
        fw_seq = row.get("fw_primer")
        rev_name = row.get("rev_name")
        rev_seq = row.get("rev_primer")

        # If forward present → update buffer
        if pd.notna(fw_name) and pd.notna(fw_seq):
            current_forward_name = fw_name
            current_forward_seq = fw_seq

        # If reverse present → create full biological pair
        if pd.notna(rev_name) and pd.notna(rev_seq):

            if current_forward_seq is None:
                raise ValueError(
                    f"Reverse primer without preceding forward at index {row['primer_pair_idx']}"
                )

            # Insert forward primer
            cur.execute("""
                INSERT INTO primer (primer_pair_id, name, sequence, direction)
                VALUES (%s,%s,%s,'forward')
                ON CONFLICT (primer_pair_id,direction) DO NOTHING
            """, (primer_pair_id, current_forward_name, current_forward_seq))

            # Insert reverse primer
            cur.execute("""
                INSERT INTO primer (primer_pair_id, name, sequence, direction)
                VALUES (%s,%s,%s,'reverse')
                ON CONFLICT (primer_pair_id,direction) DO NOTHING
            """, (primer_pair_id, rev_name, rev_seq))

        # -----------------------------
        # Primer ↔ Target
        # -----------------------------
        if target_id:
            cur.execute("""
                INSERT INTO primer_target (primer_pair_id, biological_target_id)
                VALUES (%s,%s) ON CONFLICT DO NOTHING
            """, (primer_pair_id, target_id))

        # -----------------------------
        # Locus + Coordinates
        # -----------------------------
        locus_name = row.get("locus")
        locus_id = None
        if pd.notna(locus_name):
            genbank_ac = row.get("genbank_AC")
            cur.execute("""
                INSERT INTO locus (name, genbank_accession)
                VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING id
            """, (locus_name, genbank_ac))
            res = cur.fetchone()
            if res:
                locus_id = res[0]
            else:
                cur.execute("SELECT id FROM locus WHERE name=%s", (locus_name,))
                locus_id = cur.fetchone()[0]

            for col, build in [("Hg19_coord","hg19"), ("Hg38_coord","hg38")]:
                parsed = parse_coord(row.get(col))
                if parsed:
                    chrom, start, end, strand = parsed
                    cur.execute("""
                        INSERT INTO locus_coordinate
                        (locus_id, genome_build, chromosome, start_pos, end_pos, strand)
                        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (locus_id, genome_build) DO NOTHING
                    """, (locus_id, build, chrom, start, end, strand))
            if locus_id:
                cur.execute("""
                    INSERT INTO primer_locus (primer_pair_id,locus_id)
                    VALUES (%s,%s) ON CONFLICT DO NOTHING
                """, (primer_pair_id, locus_id))

        # -----------------------------
        # Bibliography Links
        # -----------------------------
        citation_keys = tex_citations.get(row["primer_pair_idx"], [])
        for key in citation_keys:
            bib_entry = bib_entries.get(key)
            if not bib_entry:
                continue

            # Insert into bibliography
            cur.execute("""
                INSERT INTO bibliography
                (citation_key, doi, title, authors, journal, year, pubmed_id, url, raw_bibtex)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (citation_key) DO NOTHING
                RETURNING id
            """, (
                key,
                bib_entry.get("doi"),
                bib_entry.get("title"),
                bib_entry.get("author"),
                bib_entry.get("journal"),
                int(bib_entry["year"]) if "year" in bib_entry else None,
                bib_entry.get("pmid"),
                bib_entry.get("url"),
                str(bib_entry)
            ))
            res = cur.fetchone()
            if res:
                bib_id = res[0]
            else:
                cur.execute("SELECT id FROM bibliography WHERE citation_key=%s", (key,))
                bib_id = cur.fetchone()[0]

            # Link to primer_target
            if target_id:
                cur.execute("""
                    INSERT INTO primer_target_reference
                    (primer_pair_id, biological_target_id, bibliography_id)
                    VALUES (%s,%s,%s) ON CONFLICT DO NOTHING
                """, (primer_pair_id, target_id, bib_id))

            # Link to primer_locus
            if locus_id:
                cur.execute("""
                    INSERT INTO primer_locus_reference
                    (primer_pair_id, locus_id, bibliography_id)
                    VALUES (%s,%s,%s) ON CONFLICT DO NOTHING
                """, (primer_pair_id, locus_id, bib_id))

    conn.commit()
    cur.close()
    conn.close()
    print("✔ Data loaded with primers.tex citations and bibliography.bib entries.")

if __name__ == "__main__":
    main()