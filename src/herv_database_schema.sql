-- ============================================================
-- HERV Primer Relational Database Schema
-- PostgreSQL 16+
-- Updated Bibliography Model
-- ============================================================

BEGIN;

-- ============================================================
-- 1. HERV TAXONOMY
-- ============================================================

CREATE TABLE herv_family (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE herv_subgroup (
    id SERIAL PRIMARY KEY,
    herv_family_id INT NOT NULL
        REFERENCES herv_family(id)
        ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    UNIQUE (herv_family_id, name)
);

CREATE TABLE herv_component (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL, -- gag, pol, env, LTR
    description TEXT
);

-- ============================================================
-- 2. BIOLOGICAL TARGET
-- ============================================================

CREATE TABLE biological_target (
    id SERIAL PRIMARY KEY,
    herv_family_id INT NOT NULL
        REFERENCES herv_family(id)
        ON DELETE CASCADE,
    herv_subgroup_id INT
        REFERENCES herv_subgroup(id)
        ON DELETE CASCADE,
    herv_component_id INT NOT NULL
        REFERENCES herv_component(id)
        ON DELETE CASCADE,
    notes TEXT,
    UNIQUE (herv_family_id, herv_subgroup_id, herv_component_id)
);

-- ============================================================
-- 3. PRIMER PAIRS
-- ============================================================

CREATE TABLE primer_pair (
    id SERIAL PRIMARY KEY,
    pair_index INT UNIQUE NOT NULL,
    dna BOOLEAN DEFAULT FALSE,
    hervolution BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE primer (
    id SERIAL PRIMARY KEY,
    primer_pair_id INT NOT NULL
        REFERENCES primer_pair(id)
        ON DELETE CASCADE,
    name TEXT,
    sequence TEXT NOT NULL,
    direction TEXT NOT NULL
        CHECK (direction IN ('forward', 'reverse')),
    UNIQUE (primer_pair_id, direction)
);

-- ============================================================
-- 4. PRIMER ↔ BIOLOGICAL TARGET
-- ============================================================

CREATE TABLE primer_target (
    primer_pair_id INT NOT NULL
        REFERENCES primer_pair(id)
        ON DELETE CASCADE,
    biological_target_id INT NOT NULL
        REFERENCES biological_target(id)
        ON DELETE CASCADE,
    PRIMARY KEY (primer_pair_id, biological_target_id)
);

-- ============================================================
-- 5. LOCI
-- ============================================================

CREATE TABLE locus (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    genbank_accession TEXT,
    notes TEXT
);

CREATE TABLE locus_coordinate (
    id SERIAL PRIMARY KEY,
    locus_id INT NOT NULL
        REFERENCES locus(id)
        ON DELETE CASCADE,
    genome_build TEXT NOT NULL
        CHECK (genome_build IN ('hg19', 'hg38')),
    chromosome TEXT NOT NULL,
    start_pos INT NOT NULL,
    end_pos INT NOT NULL,
    strand TEXT CHECK (strand IN ('+', '-')),
    UNIQUE (locus_id, genome_build)
);

-- ============================================================
-- 6. PRIMER ↔ LOCUS
-- ============================================================

CREATE TABLE primer_locus (
    primer_pair_id INT NOT NULL
        REFERENCES primer_pair(id)
        ON DELETE CASCADE,
    locus_id INT NOT NULL
        REFERENCES locus(id)
        ON DELETE CASCADE,
    PRIMARY KEY (primer_pair_id, locus_id)
);

-- ============================================================
-- 7. BIBLIOGRAPHY (RENAMED + CLEAN MODEL)
-- ============================================================

CREATE TABLE bibliography (
    id SERIAL PRIMARY KEY,
    citation_key TEXT UNIQUE NOT NULL,  -- LaTeX/BibTeX key
    doi TEXT UNIQUE,
    title TEXT,
    authors TEXT,
    journal TEXT,
    year INT,
    pubmed_id TEXT,
    url TEXT,
    raw_bibtex TEXT,  -- optional: store full original entry
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 8. REFERENCES FOR PRIMER ↔ TARGET
-- ============================================================

CREATE TABLE primer_target_reference (
    primer_pair_id INT NOT NULL,
    biological_target_id INT NOT NULL,
    bibliography_id INT NOT NULL
        REFERENCES bibliography(id)
        ON DELETE CASCADE,
    PRIMARY KEY (primer_pair_id, biological_target_id, bibliography_id),
    FOREIGN KEY (primer_pair_id, biological_target_id)
        REFERENCES primer_target(primer_pair_id, biological_target_id)
        ON DELETE CASCADE
);


CREATE TABLE primer_pair_reference (
    primer_pair_id INT NOT NULL
        REFERENCES primer_pair(id)
        ON DELETE CASCADE,
    bibliography_id INT NOT NULL
        REFERENCES bibliography(id)
        ON DELETE CASCADE,
    PRIMARY KEY (primer_pair_id, bibliography_id)
);

-- ============================================================
-- 9. REFERENCES FOR PRIMER ↔ LOCUS
-- ============================================================

CREATE TABLE primer_locus_reference (
    primer_pair_id INT NOT NULL,
    locus_id INT NOT NULL,
    bibliography_id INT NOT NULL
        REFERENCES bibliography(id)
        ON DELETE CASCADE,
    PRIMARY KEY (primer_pair_id, locus_id, bibliography_id),
    FOREIGN KEY (primer_pair_id, locus_id)
        REFERENCES primer_locus(primer_pair_id, locus_id)
        ON DELETE CASCADE
);

-- ============================================================
-- 10. INDEXING FOR FAST API QUERIES
-- ============================================================

CREATE INDEX idx_primer_pair_index
    ON primer_pair(pair_index);

CREATE INDEX idx_biological_target_lookup
    ON biological_target (herv_family_id, herv_subgroup_id, herv_component_id);

CREATE INDEX idx_primer_target_target
    ON primer_target (biological_target_id);

CREATE INDEX idx_primer_locus_locus
    ON primer_locus (locus_id);

CREATE INDEX idx_locus_coordinate_build
    ON locus_coordinate (locus_id, genome_build);

CREATE INDEX idx_primer_sequence
    ON primer USING btree (sequence);

CREATE INDEX idx_family_name ON herv_family(name);
CREATE INDEX idx_subgroup_name ON herv_subgroup(name);
CREATE INDEX idx_component_name ON herv_component(name);

CREATE INDEX idx_bibliography_citation_key
    ON bibliography(citation_key);

CREATE INDEX idx_bibliography_doi
    ON bibliography(doi);

COMMIT;