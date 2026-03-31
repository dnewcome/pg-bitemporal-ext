\echo Use "CREATE EXTENSION pg_bitemporal" to load this file. \quit

-- ============================================================
-- pg_bitemporal v0.1
--
-- Layer 1: Allen Interval Algebra
--   All 13 Allen relations as IMMUTABLE STRICT boolean functions
--   on anyrange, plus diagnostic helpers.
--
-- Layer 2: Bitemporal Table Management
--   bitemporal.enable()       — add temporal columns, GiST index,
--                               EXCLUDE constraint, and sys-time trigger
--   bitemporal.systime_trigger() — BEFORE trigger managing sys_start/sys_end
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

-- ============================================================
-- Layer 2: Bitemporal Table Management
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bitemporal;

-- ---------------------------------------------------------------
-- bitemporal.systime_trigger()
--
-- BEFORE INSERT OR UPDATE OR DELETE trigger that manages system-time
-- columns automatically.  Installed by bitemporal.enable(); do not
-- call directly.
--
-- Trigger arguments (passed via bitemporal.enable):
--   [0]  sys_start_col
--   [1]  sys_end_col
--   [2]  mode  ('full' or 'system_only')
--   [3+] key_col(s)
-- ---------------------------------------------------------------
CREATE FUNCTION bitemporal.systime_trigger() RETURNS trigger
    LANGUAGE C
    AS '$libdir/pg_bitemporal', 'bitemporal_systime_trigger';

-- ---------------------------------------------------------------
-- bitemporal.enable(p_table, p_key_cols, ...)
--
-- Enables full bitemporality on an existing table:
--
--   1. Adds sys_start (timestamptz NOT NULL DEFAULT now()) if absent.
--   2. Adds sys_end   (timestamptz NOT NULL DEFAULT 'infinity') if absent.
--   3. Adds sys_range (tstzrange GENERATED ALWAYS AS ...) if absent.
--   4. In 'full' mode: adds valid_from, valid_to, valid_range if absent.
--   5. Creates a GiST index on the temporal columns.
--   6. Creates an EXCLUDE constraint preventing overlapping rows for
--      the same business key within the same system-time period.
--   7. Creates the BEFORE trigger that auto-manages sys_start/sys_end.
--
-- Parameters:
--   p_table      — target table (regclass)
--   p_key_cols   — business-key column names (must be NOT NULL)
--   p_sys_start  — system-time start column  (default: 'sys_start')
--   p_sys_end    — system-time end column    (default: 'sys_end')
--   p_valid_from — valid-time start column   (default: 'valid_from', full only)
--   p_valid_to   — valid-time end column     (default: 'valid_to',   full only)
--   p_mode       — 'full' (both time dims) or 'system_only'
-- ---------------------------------------------------------------
CREATE FUNCTION bitemporal.enable(
    p_table      regclass,
    p_key_cols   text[],
    p_sys_start  text    DEFAULT 'sys_start',
    p_sys_end    text    DEFAULT 'sys_end',
    p_valid_from text    DEFAULT 'valid_from',
    p_valid_to   text    DEFAULT 'valid_to',
    p_mode       text    DEFAULT 'full'
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema    text;
    v_relname   text;
    v_key_expr  text := '';
    v_trig_args text;
    i           int;
BEGIN
    /* ── Validate inputs ─────────────────────────────────────── */
    IF p_mode NOT IN ('full', 'system_only') THEN
        RAISE EXCEPTION
            'bitemporal.enable: mode must be ''full'' or ''system_only'', got ''%''',
            p_mode;
    END IF;
    IF array_length(p_key_cols, 1) IS NULL THEN
        RAISE EXCEPTION
            'bitemporal.enable: p_key_cols must be a non-empty array';
    END IF;

    /* ── Resolve schema + bare table name (for naming objects) ── */
    SELECT n.nspname, c.relname
      INTO v_schema, v_relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = p_table;

    /* ── System-time columns ─────────────────────────────────── */
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
         WHERE attrelid = p_table
           AND attname = p_sys_start
           AND NOT attisdropped
    ) THEN
        EXECUTE format(
            'ALTER TABLE %s ADD COLUMN %I timestamptz NOT NULL DEFAULT now()',
            p_table, p_sys_start);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
         WHERE attrelid = p_table
           AND attname = p_sys_end
           AND NOT attisdropped
    ) THEN
        EXECUTE format(
            'ALTER TABLE %s ADD COLUMN %I timestamptz NOT NULL'
            ' DEFAULT ''infinity''::timestamptz',
            p_table, p_sys_end);
    END IF;

    /* Generated range covering system time — used by GiST / EXCLUDE */
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
         WHERE attrelid = p_table
           AND attname = 'sys_range'
           AND NOT attisdropped
    ) THEN
        EXECUTE format(
            'ALTER TABLE %s ADD COLUMN sys_range tstzrange'
            ' GENERATED ALWAYS AS (tstzrange(%I, %I)) STORED',
            p_table, p_sys_start, p_sys_end);
    END IF;

    /* ── Valid-time columns (full mode only) ─────────────────── */
    IF p_mode = 'full' THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
             WHERE attrelid = p_table
               AND attname = p_valid_from
               AND NOT attisdropped
        ) THEN
            EXECUTE format(
                'ALTER TABLE %s ADD COLUMN %I timestamptz',
                p_table, p_valid_from);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
             WHERE attrelid = p_table
               AND attname = p_valid_to
               AND NOT attisdropped
        ) THEN
            EXECUTE format(
                'ALTER TABLE %s ADD COLUMN %I timestamptz',
                p_table, p_valid_to);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
             WHERE attrelid = p_table
               AND attname = 'valid_range'
               AND NOT attisdropped
        ) THEN
            EXECUTE format(
                'ALTER TABLE %s ADD COLUMN valid_range tstzrange'
                ' GENERATED ALWAYS AS (tstzrange(%I, %I)) STORED',
                p_table, p_valid_from, p_valid_to);
        END IF;
    END IF;

    /* ── GiST index ──────────────────────────────────────────── */
    IF p_mode = 'full' THEN
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %s USING gist (sys_range, valid_range)',
            v_relname || '_bitemporal_idx', p_table);
    ELSE
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %s USING gist (sys_range)',
            v_relname || '_bitemporal_idx', p_table);
    END IF;

    /* ── EXCLUDE constraint ──────────────────────────────────── */
    FOR i IN 1 .. array_length(p_key_cols, 1) LOOP
        v_key_expr := v_key_expr || format('%I WITH =, ', p_key_cols[i]);
    END LOOP;

    IF p_mode = 'full' THEN
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I'
            ' EXCLUDE USING gist (%s sys_range WITH &&, valid_range WITH &&)',
            p_table,
            v_relname || '_bitemporal_excl',
            v_key_expr);
    ELSE
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I'
            ' EXCLUDE USING gist (%s sys_range WITH &&)',
            p_table,
            v_relname || '_bitemporal_excl',
            v_key_expr);
    END IF;

    /* ── System-time trigger ─────────────────────────────────── */
    /* Build the argument list: sys_start, sys_end, mode, key_col(s) */
    v_trig_args := format('%L, %L, %L', p_sys_start, p_sys_end, p_mode);
    FOR i IN 1 .. array_length(p_key_cols, 1) LOOP
        v_trig_args := v_trig_args || format(', %L', p_key_cols[i]);
    END LOOP;

    EXECUTE format(
        'CREATE TRIGGER %I'
        ' BEFORE INSERT OR UPDATE OR DELETE ON %s'
        ' FOR EACH ROW EXECUTE FUNCTION bitemporal.systime_trigger(%s)',
        v_relname || '_bitemporal_trig',
        p_table,
        v_trig_args);
END;
$$;
