# pg_bitemporal — Implementation Plan

## Goal

A native C PostgreSQL extension for fully rigorous classic bitemporality:
- **System time** (transaction time): when the DB recorded the fact — auto-managed
- **Valid time** (application time): when the fact is true in the real world — user-supplied
- Full Allen interval algebra as SQL operators/functions
- Minimal table overhead (one extra column over a table that already has `created_at`)
- Optional constraints for full bitemporal integrity
- Falls back gracefully to normal queries

---

## Core Model

Every bitemporal row has four temporal endpoints stored as **two columns**:

```
sys_start   TIMESTAMPTZ  -- when row version was recorded (= created_at on append-only tables)
sys_end     TIMESTAMPTZ  -- when row version was superseded ('infinity' = current) [THE ONE EXTRA FIELD]
valid_from  TIMESTAMPTZ  -- when fact is true in the real world (user data)
valid_to    TIMESTAMPTZ  -- when fact stops being true ('infinity' = still true) (user data)
```

A generated column `sys_range` and `valid_range` (tstzrange from the pairs) enable GiST indexing
without storing a fifth/sixth column:

```sql
sys_range   tstzrange GENERATED ALWAYS AS (tstzrange(sys_start, sys_end)) STORED
valid_range tstzrange GENERATED ALWAYS AS (tstzrange(valid_from, valid_to)) STORED
```

**Append-only invariant**: rows with `sys_end < 'infinity'` are never physically updated.
All mutations go through wrapper functions that INSERT a new version and close the prior one.

---

## Architecture: Three Layers

### Layer 1 — Allen Interval Algebra (this sprint)

Pure C functions on `anyrange`. No table assumptions. Independently useful.

All 13 Allen relations as boolean functions + diagnostic helpers:

| Function | Allen Relation | Condition (half-open intervals) |
|---|---|---|
| `allen_precedes(A,B)` | p | `upper_val(A) < lower_val(B)` |
| `allen_meets(A,B)` | m | `upper_val(A) = lower_val(B)` |
| `allen_overlaps(A,B)` | o | `A.s < B.s < A.e < B.e` |
| `allen_finished_by(A,B)` | Fi | `A.s < B.s AND A.e = B.e` |
| `allen_contains(A,B)` | Di | `A.s < B.s AND B.e < A.e` |
| `allen_starts(A,B)` | s | `A.s = B.s AND A.e < B.e` |
| `allen_equals(A,B)` | e | `A.s = B.s AND A.e = B.e` |
| `allen_started_by(A,B)` | Si | `A.s = B.s AND B.e < A.e` |
| `allen_during(A,B)` | d | `B.s < A.s AND A.e < B.e` |
| `allen_finishes(A,B)` | f | `B.s < A.s AND A.e = B.e` |
| `allen_overlapped_by(A,B)` | Oi | `B.s < A.s < B.e < A.e` |
| `allen_met_by(A,B)` | Mi | `upper_val(B) = lower_val(A)` |
| `allen_preceded_by(A,B)` | Pi | `upper_val(B) < lower_val(A)` |

Extras:
- `allen_relation(anyrange, anyrange) → text` — returns the name of the relation
- `allen_relation_code(anyrange, anyrange) → char` — returns Allen's single letter code

Operators (v0.2 — names TBD after usage patterns emerge):
- Candidate symbols: `~<~`, `~m~`, `~o~`, `~d~`, `~=~`, etc.
- Deferred to avoid premature API commitment

### Layer 2 — Bitemporal Table Management (next sprint)

```sql
SELECT bitemporal.enable('my_table',
    sys_start_col  => 'created_at',   -- existing col, or NULL to auto-add
    sys_end_col    => 'sys_end',      -- the ONE new column
    valid_from_col => 'valid_from',
    valid_to_col   => 'valid_to',
    key_cols       => ARRAY['id'],
    mode           => 'full'          -- 'system_only' | 'valid_only' | 'full'
);
```

Components:
- C trigger: on INSERT with same business key, set prior row's `sys_end = now()`
- EXCLUDE constraint (requires `btree_gist`):
  - system_only: no overlapping `sys_range` for same key
  - valid_only: no overlapping `valid_range` for same key at same sys time
  - full: no overlapping `(sys_range, valid_range)` for same key
- Wrapper functions: `bitemporal.insert()`, `bitemporal.update()`, `bitemporal.delete()`
  that enforce the append-only invariant

### Layer 3 — Query Convenience (future sprint)

```sql
bitemporal.as_of(rel, sys_time, valid_time)   → setof record
bitemporal.history(rel, key_col, key_val)      → setof record
bitemporal.corrections(rel, key_col, key_val)  → setof record
bitemporal.changes_between(rel, t1, t2)        → setof record
```

---

## File Structure

```
pg-bitemporal/
├── PLAN.md                             ← this file
├── Makefile
├── pg_bitemporal.control
├── sql/
│   └── pg_bitemporal--0.1.sql          ← DDL: CREATEs for functions/operators/types
├── src/
│   ├── pg_bitemporal.c                 ← PG_MODULE_MAGIC + shared init
│   ├── allen_operators.c               ← 13 Allen relation functions
│   └── bitemporal_utils.c              ← allen_relation(), helpers (Layer 1)
└── test/
    ├── sql/
    │   └── allen_operators.sql         ← pg_regress test input
    └── expected/
        └── allen_operators.out         ← expected output
```

---

## Dependencies

- PostgreSQL 13+ (GENERATED columns, stable range API)
- `btree_gist` contrib (required for Layer 2 EXCLUDE constraints)

---

## Implementation Decisions

1. **`anyrange` not `tstzrange`**: Allen functions work on any range type for generality.
   Bitemporal table management functions are tstzrange-specific.

2. **Append-only for system time**: Never physical UPDATE/DELETE of current rows.
   `bitemporal.update()` does: INSERT new version + close prior by setting `sys_end = now()`.

3. **Generated columns for range indexing**: Store `sys_start`/`sys_end` as scalars (friendly
   to existing tooling) and auto-derive `sys_range tstzrange` for GiST. Best of both worlds.

4. **No SQL:2011 syntax**: `FOR PORTION OF`, `AS OF SYSTEM TIME` require parser patching.
   Provide equivalent semantics via plain SQL functions instead.

5. **Empty ranges return false**: All Allen functions return false for empty ranges,
   consistent with the convention that empty sets have no temporal relations.

---

## Verification Strategy

- `pg_regress` tests for all 13 Allen functions with concrete tstzrange examples
- Each relation tested: true case, false case, edge cases (touching bounds, infinite bounds)
- Exhaustiveness test: `allen_relation(a, b)` returns a non-null result for all pairs
- Layer 2: trigger-based tests verifying sys_end is closed correctly on UPDATE
