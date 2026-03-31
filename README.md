# pg_bitemporal

A native C PostgreSQL extension for rigorous classic bitemporality, built on Allen's interval algebra.

## What it does

Classic bitemporality tracks two independent time dimensions for every fact:

- **System time** — when the database recorded the fact (auto-managed)
- **Valid time** — when the fact is true in the real world (user-supplied)

This extension provides:

1. All 13 Allen interval algebra relations as SQL functions on any range type
2. `bitemporal.enable()` — adds temporal columns, indexes, constraints, and a trigger to any existing table
3. Automatic append-only enforcement via a C trigger (no raw UPDATE/DELETE on current rows)

## Requirements

- PostgreSQL 14+
- `btree_gist` contrib extension (installed automatically as a dependency)

## Build and install

```sh
make
sudo make install
```

Run regression tests:

```sh
make installcheck
```

## Quick start

```sql
CREATE EXTENSION pg_bitemporal;

CREATE TABLE employee (
    id   int  NOT NULL,
    name text NOT NULL,
    dept text
);

-- Add system-time tracking (one extra column: sys_end)
SELECT bitemporal.enable('employee', ARRAY['id'], mode => 'system_only');

-- Normal inserts work as usual
INSERT INTO employee (id, name, dept) VALUES (1, 'Alice', 'Engineering');

-- Updates are converted to append-only: old row gets sys_end stamped, new row inserted
UPDATE employee SET dept = 'Management' WHERE id = 1 AND sys_end = 'infinity';

-- Deletes are soft: sys_end is set, no physical delete
DELETE FROM employee WHERE id = 1 AND sys_end = 'infinity';

-- Full history is always available
SELECT id, name, dept, sys_start, sys_end FROM employee ORDER BY sys_start;

-- Current view
SELECT id, name, dept FROM employee WHERE sys_end = 'infinity';
```

## Tutorial

This walkthrough shows both temporal dimensions in action using an employee records scenario.

### System time only

System time tracks the history of what the database knew and when. Enable it with `system_only` mode — you get full audit history with one extra column.

```sql
CREATE EXTENSION pg_bitemporal;

CREATE TABLE employee (
    id   int  NOT NULL,
    name text NOT NULL,
    dept text
);

SELECT bitemporal.enable('employee', ARRAY['id'], mode => 'system_only');
```

Insert a row. The trigger stamps `sys_start = now()` and `sys_end = 'infinity'` automatically.

```sql
INSERT INTO employee (id, name, dept) VALUES (1, 'Alice', 'Engineering');

SELECT id, name, dept, sys_start, sys_end FROM employee;
--  id | name  |    dept     |          sys_start           | sys_end
-- ----+-------+-------------+------------------------------+---------
--   1 | Alice | Engineering | 2024-03-15 10:00:00+00       | infinity
```

Update Alice's department. The trigger cancels the physical UPDATE, closes the old row by setting its `sys_end`, and inserts a new current row.

```sql
UPDATE employee SET dept = 'Management' WHERE id = 1 AND sys_end = 'infinity';

SELECT id, name, dept, sys_start, sys_end FROM employee ORDER BY sys_start;
--  id | name  |    dept     |          sys_start           |           sys_end
-- ----+-------+-------------+------------------------------+------------------------------
--   1 | Alice | Engineering | 2024-03-15 10:00:00+00       | 2024-03-15 11:00:00+00
--   1 | Alice | Management  | 2024-03-15 11:00:00+00       | infinity
```

The old row is never gone — you can always query what was current at any past moment:

```sql
-- What did we know about Alice at 10:30?
SELECT dept FROM employee
 WHERE id = 1
   AND sys_range @> '2024-03-15 10:30:00+00'::timestamptz;
-- → Engineering

-- Current state
SELECT dept FROM employee WHERE id = 1 AND sys_end = 'infinity';
-- → Management
```

Delete Alice. The trigger performs a soft delete: `sys_end` is closed, no row is physically removed.

```sql
DELETE FROM employee WHERE id = 1 AND sys_end = 'infinity';

-- Nothing current
SELECT count(*) FROM employee WHERE sys_end = 'infinity';
-- → 0

-- Full history preserved
SELECT id, name, dept, sys_start, sys_end FROM employee ORDER BY sys_start;
--  id | name  |    dept     |       sys_start        |        sys_end
-- ----+-------+-------------+------------------------+------------------------
--   1 | Alice | Engineering | 2024-03-15 10:00:00+00 | 2024-03-15 11:00:00+00
--   1 | Alice | Management  | 2024-03-15 11:00:00+00 | 2024-03-15 12:00:00+00
```

### Full bitemporality (system time + valid time)

Valid time records when a fact is true in the real world, independently of when the database recorded it. This is where bitemporality earns its keep: you can correct past mistakes without losing the record of what you previously believed.

```sql
CREATE TABLE salary (
    emp_id  int     NOT NULL,
    amount  numeric NOT NULL
);

SELECT bitemporal.enable('salary', ARRAY['emp_id']);
-- Adds sys_start, sys_end, sys_range, valid_from, valid_to, valid_range
```

Record Alice's salary, valid for all of 2024:

```sql
INSERT INTO salary (emp_id, amount, valid_from, valid_to)
VALUES (1, 90000, '2024-01-01', '2025-01-01');
```

In March you discover her salary should have been 95000 since the start of the year. You correct it — this creates a new system-time version while preserving what you previously believed:

```sql
UPDATE salary SET amount = 95000
 WHERE emp_id = 1 AND sys_end = 'infinity';
```

Now the table holds two system-time versions of the same valid period:

```sql
SELECT amount, sys_start, sys_end, valid_from, valid_to
  FROM salary ORDER BY sys_start;
--  amount |       sys_start        |        sys_end         |    valid_from    |     valid_to
-- --------+------------------------+------------------------+------------------+------------------
--   90000 | 2024-03-15 10:00:00+00 | 2024-03-15 11:00:00+00 | 2024-01-01 00:00 | 2025-01-01 00:00
--   95000 | 2024-03-15 11:00:00+00 | infinity               | 2024-01-01 00:00 | 2025-01-01 00:00
```

This lets you answer both temporal questions independently:

```sql
-- What do we believe NOW about Alice's salary on 2024-06-01?
SELECT amount FROM salary
 WHERE emp_id = 1
   AND sys_end = 'infinity'
   AND valid_range @> '2024-06-01'::timestamptz;
-- → 95000  (the correction)

-- What did we THINK on 2024-03-15 10:30 about her salary on 2024-06-01?
SELECT amount FROM salary
 WHERE emp_id = 1
   AND sys_range @> '2024-03-15 10:30:00+00'::timestamptz
   AND valid_range @> '2024-06-01'::timestamptz;
-- → 90000  (what we believed before the correction)
```

The two questions have different answers. That's the point of bitemporality.

### Re-inserting a key

Inserting a row for a key that already has a current row automatically closes the existing one first — no need to manually delete. This is useful for bulk loads or replacing current state:

```sql
-- Alice rejoins after leaving
INSERT INTO employee (id, name, dept) VALUES (1, 'Alice', 'Sales');
-- Old current row (if any) gets sys_end = now(); new row becomes current
```

## Table structure

`bitemporal.enable()` adds these columns if they don't already exist:

| Column | Type | Description |
|---|---|---|
| `sys_start` | `timestamptz NOT NULL DEFAULT now()` | Transaction time start |
| `sys_end` | `timestamptz NOT NULL DEFAULT 'infinity'` | Transaction time end — the one extra column |
| `sys_range` | `tstzrange` (generated) | GiST-indexable range over sys_start/sys_end |
| `valid_from` | `timestamptz` | Valid time start (full mode only) |
| `valid_to` | `timestamptz` | Valid time end (full mode only) |
| `valid_range` | `tstzrange` (generated) | GiST-indexable range over valid_from/valid_to |

`sys_end = 'infinity'` is the current version. Rows with a finite `sys_end` are historical.

If your table already has a `created_at` column, pass it as `p_sys_start` — no extra column needed for system-time start.

## bitemporal.enable()

```sql
SELECT bitemporal.enable(
    p_table      => 'my_table',           -- regclass
    p_key_cols   => ARRAY['id'],          -- business key (one or more columns)
    p_sys_start  => 'sys_start',          -- default
    p_sys_end    => 'sys_end',            -- default
    p_valid_from => 'valid_from',         -- default, full mode only
    p_valid_to   => 'valid_to',           -- default, full mode only
    p_mode       => 'full'                -- 'full' or 'system_only'
);
```

What it installs:

- **Columns** — adds any missing temporal columns
- **GiST index** — on `sys_range` (and `valid_range` in full mode) for temporal queries
- **EXCLUDE constraint** — prevents overlapping temporal periods for the same business key
- **BEFORE trigger** — `bitemporal_systime_trigger` manages `sys_start`/`sys_end` automatically

## Full bitemporality (valid time)

Valid time is the user's responsibility — the extension stores and indexes it but does not set it automatically.

```sql
CREATE TABLE contract (id int NOT NULL, value text);
SELECT bitemporal.enable('contract', ARRAY['id']);  -- mode => 'full' is the default

-- Insert with valid time
INSERT INTO contract (id, value, valid_from, valid_to)
VALUES (1, 'original', '2024-01-01', '2024-12-31');

-- Correction: what we knew changed — creates a new system-time version
UPDATE contract SET value = 'corrected'
 WHERE id = 1 AND sys_end = 'infinity';

-- Point-in-time query: what was known at sys_ts about the period valid_ts?
SELECT * FROM contract
 WHERE sys_range @> 'sys_ts'::timestamptz
   AND valid_range @> 'valid_ts'::timestamptz;
```

## Allen interval algebra

All 13 Allen relations are available as SQL functions on any range type (`tstzrange`, `daterange`, `int4range`, etc.):

| Function | Code | Meaning |
|---|---|---|
| `allen_precedes(A, B)` | p | A entirely before B with a gap |
| `allen_meets(A, B)` | m | A ends exactly where B starts |
| `allen_overlaps(A, B)` | o | A starts first, partial overlap, B extends beyond |
| `allen_finished_by(A, B)` | Fi | A contains B at its tail; same end |
| `allen_contains(A, B)` | Di | B is strictly inside A |
| `allen_starts(A, B)` | s | Same start, A is shorter |
| `allen_equals(A, B)` | e | Identical intervals |
| `allen_started_by(A, B)` | Si | Same start, B is shorter |
| `allen_during(A, B)` | d | A is strictly inside B |
| `allen_finishes(A, B)` | f | A ends where B ends; A is the tail |
| `allen_overlapped_by(A, B)` | Oi | B starts first, partial overlap, A extends beyond |
| `allen_met_by(A, B)` | Mi | B ends exactly where A starts |
| `allen_preceded_by(A, B)` | Pi | B entirely before A with a gap |

The 13 relations are exhaustive and mutually exclusive for any pair of non-empty intervals.

Diagnostic helpers:

```sql
SELECT allen_relation('[2024-01-01,2024-06-01)'::tstzrange,
                      '[2024-03-01,2024-09-01)'::tstzrange);
-- → 'overlaps'

SELECT allen_relation_code('[2024-01-01,2024-06-01)'::tstzrange,
                           '[2024-03-01,2024-09-01)'::tstzrange);
-- → 'o'
```

## Design notes

- **Append-only invariant**: rows with `sys_end < 'infinity'` are never physically updated. The trigger converts `UPDATE` → close old + insert new, and `DELETE` → close old (soft delete).
- **One extra column**: if a table already has `created_at`/`updated_at`, only `sys_end` is new.
- **Generated columns** for `sys_range`/`valid_range` provide GiST indexing without storing redundant data.
- **No SQL:2011 syntax**: `FOR PORTION OF` and `AS OF SYSTEM TIME` require parser patching. Equivalent semantics are provided via plain SQL functions.
- **`anyrange`**: Allen functions work on any PostgreSQL range type, not just `tstzrange`.

## References

- Allen, J.F. (1983). "Maintaining knowledge about temporal intervals." *Communications of the ACM*, 26(11):832–843.
- Jensen, C.S., Snodgrass, R.T., Soo, M.D. (1992). "A Bitemporal Algebra." Technical Report.
