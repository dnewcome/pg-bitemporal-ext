/*
 * allen_operators.c
 *
 * Implements all 13 of Allen's interval algebra relations as boolean
 * functions on PostgreSQL range types (anyrange).
 *
 * Allen, J.F. (1983). "Maintaining knowledge about temporal intervals".
 * Communications of the ACM. 26 (11): 832–843.
 *
 * The 13 relations are exhaustive and mutually exclusive for any pair of
 * non-empty intervals.  Empty ranges return false for all relations.
 *
 * Interval notation throughout: A = [a_s, a_e), B = [b_s, b_e)
 * where _s = start value, _e = end value (half-open, canonical form).
 *
 * All functions accept anyrange so they work on tstzrange, daterange,
 * int4range, etc.
 */

#include "postgres.h"
#include "fmgr.h"
#include "pg_bitemporal_internal.h"

PG_FUNCTION_INFO_V1(allen_precedes);
PG_FUNCTION_INFO_V1(allen_meets);
PG_FUNCTION_INFO_V1(allen_overlaps);
PG_FUNCTION_INFO_V1(allen_finished_by);
PG_FUNCTION_INFO_V1(allen_contains);
PG_FUNCTION_INFO_V1(allen_starts);
PG_FUNCTION_INFO_V1(allen_equals);
PG_FUNCTION_INFO_V1(allen_started_by);
PG_FUNCTION_INFO_V1(allen_during);
PG_FUNCTION_INFO_V1(allen_finishes);
PG_FUNCTION_INFO_V1(allen_overlapped_by);
PG_FUNCTION_INFO_V1(allen_met_by);
PG_FUNCTION_INFO_V1(allen_preceded_by);

/*
 * Shared setup macro for all Allen functions.
 *
 * Deserializes both range arguments and returns false immediately for
 * empty ranges (empty ranges have no defined temporal relation).
 */
#define ALLEN_SETUP() \
	RangeType      *r1 = PG_GETARG_RANGE_P(0); \
	RangeType      *r2 = PG_GETARG_RANGE_P(1); \
	TypeCacheEntry *typcache; \
	RangeBound		lower1, upper1, lower2, upper2; \
	bool			empty1, empty2; \
	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1)); \
	range_deserialize(typcache, r1, &lower1, &upper1, &empty1); \
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2); \
	if (empty1 || empty2) \
		PG_RETURN_BOOL(false)

/*
 * Shorthand: compare the semantic values of two bounds.
 * Uses the typcache and bounds from ALLEN_SETUP().
 */
#define VCMP(b1, b2) pg_bt_cmp_bound_values(typcache, &(b1), &(b2))


/*
 * allen_precedes(A, B) — A.e < B.s
 *
 * A is entirely before B with a gap between them.
 *
 *   A: |-------|
 *   B:             |-------|
 */
Datum
allen_precedes(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(upper1, lower2) < 0);
}

/*
 * allen_meets(A, B) — A.e = B.s
 *
 * A ends exactly where B begins; no gap, no overlap.
 *
 *   A: |-------|
 *   B:         |-------|
 */
Datum
allen_meets(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(upper1, lower2) == 0);
}

/*
 * allen_overlaps(A, B) — A.s < B.s AND B.s < A.e AND A.e < B.e
 *
 * A starts before B, they share a middle portion, and B extends beyond A.
 *
 *   A: |---------|
 *   B:      |---------|
 */
Datum
allen_overlaps(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) < 0 &&
				   VCMP(lower2, upper1) < 0 &&
				   VCMP(upper1, upper2) < 0);
}

/*
 * allen_finished_by(A, B) — A.s < B.s AND A.e = B.e
 *
 * A contains B at its tail end; they finish together.
 *
 *   A: |------------|
 *   B:      |-------|
 */
Datum
allen_finished_by(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) < 0 &&
				   VCMP(upper1, upper2) == 0);
}

/*
 * allen_contains(A, B) — A.s < B.s AND B.e < A.e
 *
 * B is strictly inside A; A extends beyond B on both sides.
 *
 *   A: |---------------|
 *   B:      |-------|
 */
Datum
allen_contains(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) < 0 &&
				   VCMP(upper2, upper1) < 0);
}

/*
 * allen_starts(A, B) — A.s = B.s AND A.e < B.e
 *
 * A and B begin at the same point; A is shorter.
 *
 *   A: |-------|
 *   B: |------------|
 */
Datum
allen_starts(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) == 0 &&
				   VCMP(upper1, upper2) < 0);
}

/*
 * allen_equals(A, B) — A.s = B.s AND A.e = B.e
 *
 * A and B are identical.
 *
 *   A: |-------|
 *   B: |-------|
 */
Datum
allen_equals(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) == 0 &&
				   VCMP(upper1, upper2) == 0);
}

/*
 * allen_started_by(A, B) — A.s = B.s AND B.e < A.e
 *
 * A and B begin at the same point; B is shorter.  Inverse of starts.
 *
 *   A: |------------|
 *   B: |-------|
 */
Datum
allen_started_by(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower1, lower2) == 0 &&
				   VCMP(upper2, upper1) < 0);
}

/*
 * allen_during(A, B) — B.s < A.s AND A.e < B.e
 *
 * A is strictly inside B.  Inverse of contains.
 *
 *   A:      |-------|
 *   B: |---------------|
 */
Datum
allen_during(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower2, lower1) < 0 &&
				   VCMP(upper1, upper2) < 0);
}

/*
 * allen_finishes(A, B) — B.s < A.s AND A.e = B.e
 *
 * A ends where B ends; A is the tail portion of B.  Inverse of finished-by.
 *
 *   A:      |-------|
 *   B: |------------|
 */
Datum
allen_finishes(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower2, lower1) < 0 &&
				   VCMP(upper1, upper2) == 0);
}

/*
 * allen_overlapped_by(A, B) — B.s < A.s AND A.s < B.e AND B.e < A.e
 *
 * B starts before A, they share a middle portion, and A extends beyond B.
 * Inverse of overlaps.
 *
 *   A:      |---------|
 *   B: |---------|
 */
Datum
allen_overlapped_by(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(lower2, lower1) < 0 &&
				   VCMP(lower1, upper2) < 0 &&
				   VCMP(upper2, upper1) < 0);
}

/*
 * allen_met_by(A, B) — B.e = A.s
 *
 * B ends exactly where A begins.  Inverse of meets.
 *
 *   A:         |-------|
 *   B: |-------|
 */
Datum
allen_met_by(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(upper2, lower1) == 0);
}

/*
 * allen_preceded_by(A, B) — B.e < A.s
 *
 * B is entirely before A with a gap.  Inverse of precedes.
 *
 *   A:             |-------|
 *   B: |-------|
 */
Datum
allen_preceded_by(PG_FUNCTION_ARGS)
{
	ALLEN_SETUP();
	PG_RETURN_BOOL(VCMP(upper2, lower1) < 0);
}
