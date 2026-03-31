-- bitemporal.enable() and systime trigger regression tests
-- Relies on pg_bitemporal already being installed by allen_operators test.

\pset format unaligned
\pset tuples_only on

-- ============================================================
-- 1. system_only mode
-- ============================================================

CREATE TABLE bt_emp (
    id   int  NOT NULL,
    name text NOT NULL
);

DO $$
BEGIN
    PERFORM bitemporal.enable(
        'bt_emp'::regclass,
        ARRAY['id'],
        'sys_start', 'sys_end', 'valid_from', 'valid_to',
        'system_only'
    );
END
$$;

-- Temporal columns sys_start, sys_end, sys_range added
\echo '-- sys_cols_added'
SELECT count(*) FROM pg_attribute
 WHERE attrelid = 'bt_emp'::regclass
   AND attname IN ('sys_start', 'sys_end', 'sys_range')
   AND NOT attisdropped;

-- valid_range NOT added in system_only mode
\echo '-- no_valid_range'
SELECT count(*) FROM pg_attribute
 WHERE attrelid = 'bt_emp'::regclass
   AND attname = 'valid_range'
   AND NOT attisdropped;

-- Trigger was installed
\echo '-- trigger_installed'
SELECT count(*) FROM pg_trigger
 WHERE tgrelid = 'bt_emp'::regclass;

-- ---- INSERT: sys_start stamped, sys_end = infinity ----
\echo '-- after_insert_total'
INSERT INTO bt_emp (id, name) VALUES (1, 'Alice');
SELECT count(*) FROM bt_emp;

\echo '-- after_insert_current'
SELECT count(*) FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

-- ---- second INSERT on same key: old row closed, new row current ----
\echo '-- after_second_insert_total'
INSERT INTO bt_emp (id, name) VALUES (1, 'Alice B');
SELECT count(*) FROM bt_emp;

\echo '-- after_second_insert_current'
SELECT count(*) FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

\echo '-- current_name'
SELECT name FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

-- ---- UPDATE: cancelled, history retained, new version current ----
\echo '-- after_update_total'
UPDATE bt_emp SET name = 'Alice C'
 WHERE id = 1 AND sys_end = 'infinity'::timestamptz;
SELECT count(*) FROM bt_emp;

\echo '-- after_update_current'
SELECT count(*) FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

\echo '-- updated_name'
SELECT name FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

-- ---- DELETE: soft delete (physical row count unchanged) ----
\echo '-- after_delete_current'
DELETE FROM bt_emp WHERE id = 1 AND sys_end = 'infinity'::timestamptz;
SELECT count(*) FROM bt_emp WHERE sys_end = 'infinity'::timestamptz;

\echo '-- after_delete_total'
SELECT count(*) FROM bt_emp;

DROP TABLE bt_emp;

-- ============================================================
-- 2. full mode (both system time + valid time)
-- ============================================================

CREATE TABLE bt_contract (
    id    int  NOT NULL,
    value text NOT NULL
);

DO $$
BEGIN
    PERFORM bitemporal.enable('bt_contract'::regclass, ARRAY['id']);
END
$$;

-- All 6 temporal columns added
\echo '-- full_cols_added'
SELECT count(*) FROM pg_attribute
 WHERE attrelid = 'bt_contract'::regclass
   AND attname IN ('sys_start', 'sys_end', 'sys_range',
                   'valid_from', 'valid_to', 'valid_range')
   AND NOT attisdropped;

-- ---- INSERT with valid time ----
\echo '-- full_after_insert'
INSERT INTO bt_contract (id, value, valid_from, valid_to)
VALUES (1, 'v1', '2025-01-01'::timestamptz, '2025-12-31'::timestamptz);
SELECT count(*) FROM bt_contract;
SELECT value FROM bt_contract WHERE sys_end = 'infinity'::timestamptz;

-- ---- UPDATE: new system version, valid time preserved ----
\echo '-- full_after_update'
UPDATE bt_contract SET value = 'v2'
 WHERE id = 1 AND sys_end = 'infinity'::timestamptz;
SELECT count(*) FROM bt_contract;
SELECT value FROM bt_contract WHERE sys_end = 'infinity'::timestamptz;

-- ---- DELETE: soft delete ----
\echo '-- full_after_delete_current'
DELETE FROM bt_contract WHERE id = 1 AND sys_end = 'infinity'::timestamptz;
SELECT count(*) FROM bt_contract WHERE sys_end = 'infinity'::timestamptz;

\echo '-- full_after_delete_total'
SELECT count(*) FROM bt_contract;

DROP TABLE bt_contract;
