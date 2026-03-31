/*
 * bitemporal_utils.c
 *
 * Diagnostic and utility functions built on the Allen algebra.
 *
 *   allen_relation(anyrange, anyrange) → text
 *     Returns the name of the unique Allen relation between two intervals.
 *     Returns NULL if either argument is an empty range.
 *
 *   allen_relation_code(anyrange, anyrange) → text
 *     Returns Allen's canonical single-letter code for the relation:
 *       p  m  o  Fi  Di  s  e  Si  d  f  Oi  Mi  Pi
 *     (uppercase letters mark the "inverse" relations in Allen's notation)
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "pg_bitemporal_internal.h"

PG_FUNCTION_INFO_V1(allen_relation);
PG_FUNCTION_INFO_V1(allen_relation_code);

/*
 * Classify the Allen relation between non-empty intervals A and B.
 *
 * The classification tree uses two key comparisons:
 *   ul  = upper(A) vs lower(B)   [determines precedes/meets/follows trio]
 *   ul2 = upper(B) vs lower(A)   [same, from B's perspective]
 *
 * When both intervals genuinely overlap (ul > 0 AND ul2 > 0), two more
 * comparisons suffice to identify the exact relation:
 *   ls  = lower(A) vs lower(B)   [who starts first]
 *   ue  = upper(A) vs upper(B)   [who ends first]
 *
 * Returns: pointer to a static string naming the relation.
 */
static const char *
classify_allen_relation(TypeCacheEntry *typcache,
                        const RangeBound *lower1, const RangeBound *upper1,
                        const RangeBound *lower2, const RangeBound *upper2)
{
	int			ul  = pg_bt_cmp_bound_values(typcache, upper1, lower2);
	int			ul2 = pg_bt_cmp_bound_values(typcache, upper2, lower1);
	int			ls,
				ue;

	/* ── Non-overlapping cases ─────────────────────────────────────── */
	if (ul < 0)
		return "precedes";       /* A entirely before B, gap exists     */
	if (ul == 0)
		return "meets";          /* A ends exactly where B starts       */
	if (ul2 == 0)
		return "met-by";         /* B ends exactly where A starts       */
	if (ul2 < 0)
		return "preceded-by";    /* B entirely before A, gap exists     */

	/* ── Overlapping cases (ul > 0 AND ul2 > 0) ────────────────────── */
	ls = pg_bt_cmp_bound_values(typcache, lower1, lower2);
	ue = pg_bt_cmp_bound_values(typcache, upper1, upper2);

	if (ls < 0)
	{
		/* A starts first */
		if (ue < 0)  return "overlaps";      /* A:s<B:s, A:e<B:e */
		if (ue == 0) return "finished-by";   /* A:s<B:s, A:e=B:e */
		return "contains";                   /* A:s<B:s, A:e>B:e */
	}
	if (ls == 0)
	{
		/* Same start */
		if (ue < 0)  return "starts";        /* A:s=B:s, A:e<B:e */
		if (ue == 0) return "equals";        /* A:s=B:s, A:e=B:e */
		return "started-by";                 /* A:s=B:s, A:e>B:e */
	}
	/* ls > 0: B starts first */
	if (ue > 0)  return "overlapped-by";     /* B:s<A:s, A:e>B:e */
	if (ue == 0) return "finishes";          /* B:s<A:s, A:e=B:e */
	return "during";                         /* B:s<A:s, A:e<B:e */
}

/*
 * allen_relation(anyrange, anyrange) → text
 */
Datum
allen_relation(PG_FUNCTION_ARGS)
{
	RangeType      *r1 = PG_GETARG_RANGE_P(0);
	RangeType      *r2 = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	RangeBound		lower1, upper1, lower2, upper2;
	bool			empty1, empty2;
	const char	   *rel;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));
	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	if (empty1 || empty2)
		PG_RETURN_NULL();

	rel = classify_allen_relation(typcache,
								  &lower1, &upper1,
								  &lower2, &upper2);
	PG_RETURN_TEXT_P(cstring_to_text(rel));
}

/*
 * allen_relation_code(anyrange, anyrange) → text
 *
 * Returns Allen's canonical code string (1-2 chars):
 *   p  m  o  Fi  Di  s  e  Si  d  f  Oi  Mi  Pi
 */
Datum
allen_relation_code(PG_FUNCTION_ARGS)
{
	RangeType      *r1 = PG_GETARG_RANGE_P(0);
	RangeType      *r2 = PG_GETARG_RANGE_P(1);
	TypeCacheEntry *typcache;
	RangeBound		lower1, upper1, lower2, upper2;
	bool			empty1, empty2;
	const char	   *rel;
	const char	   *code;

	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));
	range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
	range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

	if (empty1 || empty2)
		PG_RETURN_NULL();

	rel = classify_allen_relation(typcache,
								  &lower1, &upper1,
								  &lower2, &upper2);

	if      (strcmp(rel, "precedes")      == 0) code = "p";
	else if (strcmp(rel, "meets")         == 0) code = "m";
	else if (strcmp(rel, "overlaps")      == 0) code = "o";
	else if (strcmp(rel, "finished-by")   == 0) code = "Fi";
	else if (strcmp(rel, "contains")      == 0) code = "Di";
	else if (strcmp(rel, "starts")        == 0) code = "s";
	else if (strcmp(rel, "equals")        == 0) code = "e";
	else if (strcmp(rel, "started-by")    == 0) code = "Si";
	else if (strcmp(rel, "during")        == 0) code = "d";
	else if (strcmp(rel, "finishes")      == 0) code = "f";
	else if (strcmp(rel, "overlapped-by") == 0) code = "Oi";
	else if (strcmp(rel, "met-by")        == 0) code = "Mi";
	else if (strcmp(rel, "preceded-by")   == 0) code = "Pi";
	else code = "?";  /* should never happen */

	PG_RETURN_TEXT_P(cstring_to_text(code));
}
