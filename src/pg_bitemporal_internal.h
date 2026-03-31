#ifndef PG_BITEMPORAL_INTERNAL_H
#define PG_BITEMPORAL_INTERNAL_H

#include "utils/rangetypes.h"
#include "utils/typcache.h"

/*
 * Compare the semantic values of two range bounds.
 *
 * Lower-infinite bounds are treated as -infinity; upper-infinite bounds as
 * +infinity.  Inclusivity is intentionally ignored: we compare only the raw
 * values (the "points" on the timeline), not their bounding role.  This is
 * correct for Allen's algebra, which is defined over the open-interval model
 * where boundary semantics are resolved by the half-open canonical form that
 * PostgreSQL uses for all range types.
 *
 * Returns: negative if b1 < b2, zero if equal, positive if b1 > b2.
 */
static inline int
pg_bt_cmp_bound_values(TypeCacheEntry *typcache,
					   const RangeBound *b1, const RangeBound *b2)
{
	bool		b1_neg_inf = b1->infinite && b1->lower;
	bool		b1_pos_inf = b1->infinite && !b1->lower;
	bool		b2_neg_inf = b2->infinite && b2->lower;
	bool		b2_pos_inf = b2->infinite && !b2->lower;

	if (b1_neg_inf && b2_neg_inf)
		return 0;
	if (b1_pos_inf && b2_pos_inf)
		return 0;
	if (b1_neg_inf || b2_pos_inf)
		return -1;
	if (b1_pos_inf || b2_neg_inf)
		return 1;

	return DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
										   typcache->rng_collation,
										   b1->val, b2->val));
}

#endif							/* PG_BITEMPORAL_INTERNAL_H */
