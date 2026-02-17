import os
import pandas as pd
import psycopg2
import re


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "hervdb")
DB_USER = os.getenv("DB_USER", "herv")
DB_PASS = os.getenv("DB_PASSWORD", "hervpass")

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "primERV.tsv"
)

DB_URL = f"""
dbname={DB_NAME}
user={DB_USER}
password={DB_PASS}
host={DB_HOST}
port={DB_PORT}
"""


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def parse_coord(coord_string):
    """
    Parse coordinates like:
    chr7:12345-12500(+)
    """
    if not coord_string or pd.isna(coord_string):
        return None

    coord_string = str(coord_string).strip()

    match = re.match(r"(chr\w+):(\d+)-(\d+)\(?([+-])?\)?", coord_string)
    if not match:
        return None

    chrom, start, end, strand = match.groups()
    return chrom, int(start), int(end), strand


def get_or_create(cur, table, unique_col, value):
    if value is None or value == "":
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

    cur.execute(
        f"SELECT id FROM {table} WHERE {unique_col} = %s",
        (value,)
    )
    return cur.fetchone()[0]


# ---------------------------------------------------------
# Main Loader
# ---------------------------------------------------------

def main():

    print(f"Loading TSV from: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH, sep="\t")

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    for _, row in df.iterrows():

        # -------------------------------------------------
        # HERV Family
        # -------------------------------------------------
        family_id = get_or_create(
            cur,
            "herv_family",
            "name",
            row["ERV_group"]
        )

        # -------------------------------------------------
        # Subgroup
        # -------------------------------------------------
        subgroup_id = None
        if pd.notna(row["ERV_subgroup"]):

            cur.execute("""
                INSERT INTO herv_subgroup (herv_family_id, name)
                VALUES (%s, %s)
                ON CONFLICT (herv_family_id, name)
                DO NOTHING
                RETURNING id;
            """, (family_id, row["ERV_subgroup"]))

            result = cur.fetchone()
            if result:
                subgroup_id = result[0]
            else:
                cur.execute("""
                    SELECT id FROM herv_subgroup
                    WHERE herv_family_id=%s AND name=%s
                """, (family_id, row["ERV_subgroup"]))
                subgroup_id = cur.fetchone()[0]

        # -------------------------------------------------
        # Component
        # -------------------------------------------------
        component_id = get_or_create(
            cur,
            "herv_component",
            "name",
            row["ERV_component"]
        )

        # -------------------------------------------------
        # Biological Target
        # -------------------------------------------------
        cur.execute("""
            INSERT INTO biological_target
            (herv_family_id, herv_subgroup_id, herv_component_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (herv_family_id, herv_subgroup_id, herv_component_id)
            DO NOTHING
            RETURNING id;
        """, (family_id, subgroup_id, component_id))

        result = cur.fetchone()
        if result:
            target_id = result[0]
        else:
            cur.execute("""
                SELECT id FROM biological_target
                WHERE herv_family_id=%s
                AND herv_subgroup_id IS NOT DISTINCT FROM %s
                AND herv_component_id=%s
            """, (family_id, subgroup_id, component_id))
            target_id = cur.fetchone()[0]

        # -------------------------------------------------
        # Primer Pair
        # -------------------------------------------------
        cur.execute("""
            INSERT INTO primer_pair (pair_index, dna, hervolution)
            VALUES (%s, %s, %s)
            ON CONFLICT (pair_index) DO NOTHING
            RETURNING id;
        """, (
            row["primer_pair_idx"],
            bool(row["DNA"]),
            bool(row["HERVolution"])
        ))

        result = cur.fetchone()
        if result:
            primer_pair_id = result[0]
        else:
            cur.execute("""
                SELECT id FROM primer_pair
                WHERE pair_index=%s
            """, (row["primer_pair_idx"],))
            primer_pair_id = cur.fetchone()[0]

        # -------------------------------------------------
        # Forward Primer
        # -------------------------------------------------
        cur.execute("""
            INSERT INTO primer (primer_pair_id, name, sequence, direction)
            VALUES (%s, %s, %s, 'forward')
            ON CONFLICT (primer_pair_id, direction)
            DO NOTHING;
        """, (
            primer_pair_id,
            row["fw_name"],
            row["fw_primer"]
        ))

        # -------------------------------------------------
        # Reverse Primer
        # -------------------------------------------------
        cur.execute("""
            INSERT INTO primer (primer_pair_id, name, sequence, direction)
            VALUES (%s, %s, %s, 'reverse')
            ON CONFLICT (primer_pair_id, direction)
            DO NOTHING;
        """, (
            primer_pair_id,
            row["rev_name"],
            row["rev_primer"]
        ))

        # -------------------------------------------------
        # Link Primer ↔ Target
        # -------------------------------------------------
        cur.execute("""
            INSERT INTO primer_target (primer_pair_id, biological_target_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (primer_pair_id, target_id))

        # -------------------------------------------------
        # Locus + Coordinates
        # -------------------------------------------------
        if pd.notna(row["locus"]):

            cur.execute("""
                INSERT INTO locus (name, genbank_accession)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id;
            """, (row["locus"], row["genbank_AC"]))

            result = cur.fetchone()
            if result:
                locus_id = result[0]
            else:
                cur.execute(
                    "SELECT id FROM locus WHERE name=%s",
                    (row["locus"],)
                )
                locus_id = cur.fetchone()[0]

            for build_col, build_name in [
                ("Hg19_coord", "hg19"),
                ("Hg38_coord", "hg38")
            ]:
                parsed = parse_coord(row.get(build_col))
                if parsed:
                    chrom, start, end, strand = parsed
                    cur.execute("""
                        INSERT INTO locus_coordinate
                        (locus_id, genome_build, chromosome, start_pos, end_pos, strand)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (locus_id, genome_build)
                        DO NOTHING;
                    """, (locus_id, build_name, chrom, start, end, strand))

            cur.execute("""
                INSERT INTO primer_locus (primer_pair_id, locus_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (primer_pair_id, locus_id))

    conn.commit()
    cur.close()
    conn.close()

    print("✔ Data successfully loaded.")


if __name__ == "__main__":
    main()

