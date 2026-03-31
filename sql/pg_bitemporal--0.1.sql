\echo Use "CREATE EXTENSION pg_bitemporal" to load this file. \quit

-- ============================================================
-- pg_bitemporal v0.1
--
-- Layer 1: Allen Interval Algebra
--   All 13 Allen relations as IMMUTABLE STRICT boolean functions
--   on anyrange, plus diagnostic helpers.
--
-- Layer 2 (bitemporal table management) is in v0.2.
-- ============================================================

-- ------------------------------------------------------------
-- Allen's 13 interval relations
-- ------------------------------------------------------------

-- precedes (p): A entirely before B, gap exists
--   A: |-------|
--   B:             |-------|
CREATE FUNCTION allen_precedes(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_precedes';

-- meets (m): A ends exactly where B starts, no gap
--   A: |-------|
--   B:         |-------|
CREATE FUNCTION allen_meets(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_meets';

-- overlaps (o): A starts first, partial overlap, B extends beyond
--   A: |---------|
--   B:      |---------|
CREATE FUNCTION allen_overlaps(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_overlaps';

-- finished-by (Fi): A contains B at its tail; same end point
--   A: |------------|
--   B:      |-------|
CREATE FUNCTION allen_finished_by(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_finished_by';

-- contains (Di): B is strictly inside A
--   A: |---------------|
--   B:      |-------|
CREATE FUNCTION allen_contains(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_contains';

-- starts (s): same start, A is shorter
--   A: |-------|
--   B: |------------|
CREATE FUNCTION allen_starts(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_starts';

-- equals (e): identical intervals
--   A: |-------|
--   B: |-------|
CREATE FUNCTION allen_equals(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_equals';

-- started-by (Si): same start, B is shorter; inverse of starts
--   A: |------------|
--   B: |-------|
CREATE FUNCTION allen_started_by(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_started_by';

-- during (d): A is strictly inside B; inverse of contains
--   A:      |-------|
--   B: |---------------|
CREATE FUNCTION allen_during(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_during';

-- finishes (f): A ends where B ends, A is a tail portion; inverse of finished-by
--   A:      |-------|
--   B: |------------|
CREATE FUNCTION allen_finishes(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_finishes';

-- overlapped-by (Oi): B starts first, partial overlap, A extends beyond; inverse of overlaps
--   A:      |---------|
--   B: |---------|
CREATE FUNCTION allen_overlapped_by(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_overlapped_by';

-- met-by (Mi): B ends exactly where A starts; inverse of meets
--   A:         |-------|
--   B: |-------|
CREATE FUNCTION allen_met_by(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_met_by';

-- preceded-by (Pi): B entirely before A, gap exists; inverse of precedes
--   A:             |-------|
--   B: |-------|
CREATE FUNCTION allen_preceded_by(anyrange, anyrange) RETURNS boolean
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_preceded_by';

-- ------------------------------------------------------------
-- Diagnostic helpers
-- ------------------------------------------------------------

-- Returns the name of the Allen relation between A and B.
-- Returns NULL if either argument is empty.
CREATE FUNCTION allen_relation(anyrange, anyrange) RETURNS text
    LANGUAGE C IMMUTABLE PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_relation';

-- Returns Allen's canonical code for the relation (p, m, o, Fi, Di, s, e, Si, d, f, Oi, Mi, Pi).
-- Returns NULL if either argument is empty.
CREATE FUNCTION allen_relation_code(anyrange, anyrange) RETURNS text
    LANGUAGE C IMMUTABLE PARALLEL SAFE
    AS '$libdir/pg_bitemporal', 'allen_relation_code';

-- ------------------------------------------------------------
-- Convenience: check that the 13 relations partition all pairs
-- ------------------------------------------------------------

-- Returns true if the relation holds for the 'overlapping' category
-- (i.e. the built-in && operator), for cross-checking.
-- (o, Fi, Di, s, e, Si, d, f, Oi all imply &&; p, m, Mi, Pi do not)
CREATE FUNCTION allen_any_overlap(anyrange, anyrange) RETURNS boolean
    LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
    AS $$
        SELECT allen_relation($1, $2) IN (
            'overlaps', 'finished-by', 'contains',
            'starts', 'equals', 'started-by',
            'during', 'finishes', 'overlapped-by'
        );
    $$;
