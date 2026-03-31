-- Allen interval algebra regression tests
-- Uses tstzrange for concreteness; all functions accept anyrange.

CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION pg_bitemporal;

-- Use unaligned, header-free output so expected file is formatting-independent.
\pset format unaligned
\pset tuples_only on

-- ============================================================
-- 1. allen_precedes: upper_val(A) < lower_val(B)
-- ============================================================
\echo '-- precedes'
-- true: clear gap between [Jan,Apr) and [Jul,Oct)
SELECT allen_precedes('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-07-01,2000-10-01)'::tstzrange);
-- false: reversed
SELECT allen_precedes('[2000-07-01,2000-10-01)'::tstzrange,
                      '[2000-01-01,2000-04-01)'::tstzrange);
-- false: touching (that is 'meets')
SELECT allen_precedes('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-04-01,2000-07-01)'::tstzrange);

-- ============================================================
-- 2. allen_meets: upper_val(A) = lower_val(B)
-- ============================================================
\echo '-- meets'
-- true: [Jan,Apr) touches [Apr,Jul)
SELECT allen_meets('[2000-01-01,2000-04-01)'::tstzrange,
                   '[2000-04-01,2000-07-01)'::tstzrange);
-- false: gap between them
SELECT allen_meets('[2000-01-01,2000-04-01)'::tstzrange,
                   '[2000-07-01,2000-10-01)'::tstzrange);
-- false: overlapping
SELECT allen_meets('[2000-01-01,2000-04-01)'::tstzrange,
                   '[2000-03-01,2000-07-01)'::tstzrange);

-- ============================================================
-- 3. allen_overlaps: A.s < B.s < A.e < B.e
-- ============================================================
\echo '-- overlaps'
-- true: [Jan,Apr) and [Mar,Jun)
SELECT allen_overlaps('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-03-01,2000-06-01)'::tstzrange);
-- false: reversed (that is overlapped-by)
SELECT allen_overlaps('[2000-03-01,2000-06-01)'::tstzrange,
                      '[2000-01-01,2000-04-01)'::tstzrange);
-- false: B completely after A
SELECT allen_overlaps('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-07-01,2000-10-01)'::tstzrange);

-- ============================================================
-- 4. allen_finished_by: A.s < B.s AND A.e = B.e
-- ============================================================
\echo '-- finished-by'
-- true: [Jan,Apr) finished-by [Feb,Apr)
SELECT allen_finished_by('[2000-01-01,2000-04-01)'::tstzrange,
                         '[2000-02-01,2000-04-01)'::tstzrange);
-- false: reversed (that is finishes)
SELECT allen_finished_by('[2000-02-01,2000-04-01)'::tstzrange,
                         '[2000-01-01,2000-04-01)'::tstzrange);
-- false: different ends
SELECT allen_finished_by('[2000-01-01,2000-04-01)'::tstzrange,
                         '[2000-02-01,2000-06-01)'::tstzrange);

-- ============================================================
-- 5. allen_contains: A.s < B.s AND B.e < A.e
-- ============================================================
\echo '-- contains'
-- true: [Jan,Oct) contains [Mar,Jun)
SELECT allen_contains('[2000-01-01,2000-10-01)'::tstzrange,
                      '[2000-03-01,2000-06-01)'::tstzrange);
-- false: reversed (that is during)
SELECT allen_contains('[2000-03-01,2000-06-01)'::tstzrange,
                      '[2000-01-01,2000-10-01)'::tstzrange);
-- false: finished-by is not contains (same end)
SELECT allen_contains('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-02-01,2000-04-01)'::tstzrange);

-- ============================================================
-- 6. allen_starts: A.s = B.s AND A.e < B.e
-- ============================================================
\echo '-- starts'
-- true: [Jan,Apr) starts [Jan,Jun)
SELECT allen_starts('[2000-01-01,2000-04-01)'::tstzrange,
                    '[2000-01-01,2000-06-01)'::tstzrange);
-- false: reversed (that is started-by)
SELECT allen_starts('[2000-01-01,2000-06-01)'::tstzrange,
                    '[2000-01-01,2000-04-01)'::tstzrange);
-- false: different starts
SELECT allen_starts('[2000-02-01,2000-04-01)'::tstzrange,
                    '[2000-01-01,2000-06-01)'::tstzrange);

-- ============================================================
-- 7. allen_equals: A.s = B.s AND A.e = B.e
-- ============================================================
\echo '-- equals'
-- true
SELECT allen_equals('[2000-01-01,2000-04-01)'::tstzrange,
                    '[2000-01-01,2000-04-01)'::tstzrange);
-- false: different end
SELECT allen_equals('[2000-01-01,2000-04-01)'::tstzrange,
                    '[2000-01-01,2000-04-02)'::tstzrange);

-- ============================================================
-- 8. allen_started_by: A.s = B.s AND B.e < A.e
-- ============================================================
\echo '-- started-by'
-- true: [Jan,Jun) started-by [Jan,Apr)
SELECT allen_started_by('[2000-01-01,2000-06-01)'::tstzrange,
                        '[2000-01-01,2000-04-01)'::tstzrange);
-- false: reversed (that is starts)
SELECT allen_started_by('[2000-01-01,2000-04-01)'::tstzrange,
                        '[2000-01-01,2000-06-01)'::tstzrange);

-- ============================================================
-- 9. allen_during: B.s < A.s AND A.e < B.e
-- ============================================================
\echo '-- during'
-- true: [Mar,Jun) during [Jan,Oct)
SELECT allen_during('[2000-03-01,2000-06-01)'::tstzrange,
                    '[2000-01-01,2000-10-01)'::tstzrange);
-- false: reversed (that is contains)
SELECT allen_during('[2000-01-01,2000-10-01)'::tstzrange,
                    '[2000-03-01,2000-06-01)'::tstzrange);
-- false: only partial overlap
SELECT allen_during('[2000-03-01,2000-06-01)'::tstzrange,
                    '[2000-01-01,2000-04-01)'::tstzrange);

-- ============================================================
-- 10. allen_finishes: B.s < A.s AND A.e = B.e
-- ============================================================
\echo '-- finishes'
-- true: [Feb,Apr) finishes [Jan,Apr)
SELECT allen_finishes('[2000-02-01,2000-04-01)'::tstzrange,
                      '[2000-01-01,2000-04-01)'::tstzrange);
-- false: reversed (that is finished-by)
SELECT allen_finishes('[2000-01-01,2000-04-01)'::tstzrange,
                      '[2000-02-01,2000-04-01)'::tstzrange);

-- ============================================================
-- 11. allen_overlapped_by: B.s < A.s < B.e < A.e
-- ============================================================
\echo '-- overlapped-by'
-- true: [Mar,Jun) overlapped-by [Jan,Apr)
SELECT allen_overlapped_by('[2000-03-01,2000-06-01)'::tstzrange,
                           '[2000-01-01,2000-04-01)'::tstzrange);
-- false: reversed (that is overlaps)
SELECT allen_overlapped_by('[2000-01-01,2000-04-01)'::tstzrange,
                           '[2000-03-01,2000-06-01)'::tstzrange);

-- ============================================================
-- 12. allen_met_by: upper_val(B) = lower_val(A)
-- ============================================================
\echo '-- met-by'
-- true: [Apr,Jul) met-by [Jan,Apr)
SELECT allen_met_by('[2000-04-01,2000-07-01)'::tstzrange,
                    '[2000-01-01,2000-04-01)'::tstzrange);
-- false: gap
SELECT allen_met_by('[2000-04-01,2000-07-01)'::tstzrange,
                    '[2000-01-01,2000-03-01)'::tstzrange);

-- ============================================================
-- 13. allen_preceded_by: upper_val(B) < lower_val(A)
-- ============================================================
\echo '-- preceded-by'
-- true: [Jul,Oct) preceded-by [Jan,Apr)
SELECT allen_preceded_by('[2000-07-01,2000-10-01)'::tstzrange,
                         '[2000-01-01,2000-04-01)'::tstzrange);
-- false: reversed
SELECT allen_preceded_by('[2000-01-01,2000-04-01)'::tstzrange,
                         '[2000-07-01,2000-10-01)'::tstzrange);

-- ============================================================
-- allen_relation: returns the name of the relation
-- ============================================================
\echo '-- allen_relation names'
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-07-01,2000-10-01)'::tstzrange);  -- precedes
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-04-01,2000-07-01)'::tstzrange);  -- meets
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-03-01,2000-06-01)'::tstzrange);  -- overlaps
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-02-01,2000-04-01)'::tstzrange);  -- finished-by
SELECT allen_relation('[2000-01-01,2000-10-01)'::tstzrange, '[2000-03-01,2000-06-01)'::tstzrange);  -- contains
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-01-01,2000-06-01)'::tstzrange);  -- starts
SELECT allen_relation('[2000-01-01,2000-04-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- equals
SELECT allen_relation('[2000-01-01,2000-06-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- started-by
SELECT allen_relation('[2000-03-01,2000-06-01)'::tstzrange, '[2000-01-01,2000-10-01)'::tstzrange);  -- during
SELECT allen_relation('[2000-02-01,2000-04-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- finishes
SELECT allen_relation('[2000-03-01,2000-06-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- overlapped-by
SELECT allen_relation('[2000-04-01,2000-07-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- met-by
SELECT allen_relation('[2000-07-01,2000-10-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- preceded-by

-- ============================================================
-- allen_relation_code: single-letter Allen codes
-- ============================================================
\echo '-- allen_relation_code'
SELECT allen_relation_code('[2000-01-01,2000-04-01)'::tstzrange, '[2000-07-01,2000-10-01)'::tstzrange);  -- p
SELECT allen_relation_code('[2000-01-01,2000-04-01)'::tstzrange, '[2000-04-01,2000-07-01)'::tstzrange);  -- m
SELECT allen_relation_code('[2000-01-01,2000-04-01)'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);  -- e
SELECT allen_relation_code('[2000-03-01,2000-06-01)'::tstzrange, '[2000-01-01,2000-10-01)'::tstzrange);  -- d
SELECT allen_relation_code('[2000-01-01,2000-10-01)'::tstzrange, '[2000-03-01,2000-06-01)'::tstzrange);  -- Di

-- ============================================================
-- Exhaustiveness: every pair has exactly 1 true relation.
-- ============================================================
\echo '-- exhaustiveness (all true_count should be 1)'
SELECT
    allen_relation(a::tstzrange, b::tstzrange) AS rel,
    (allen_precedes(a::tstzrange, b::tstzrange)::int +
     allen_meets(a::tstzrange, b::tstzrange)::int +
     allen_overlaps(a::tstzrange, b::tstzrange)::int +
     allen_finished_by(a::tstzrange, b::tstzrange)::int +
     allen_contains(a::tstzrange, b::tstzrange)::int +
     allen_starts(a::tstzrange, b::tstzrange)::int +
     allen_equals(a::tstzrange, b::tstzrange)::int +
     allen_started_by(a::tstzrange, b::tstzrange)::int +
     allen_during(a::tstzrange, b::tstzrange)::int +
     allen_finishes(a::tstzrange, b::tstzrange)::int +
     allen_overlapped_by(a::tstzrange, b::tstzrange)::int +
     allen_met_by(a::tstzrange, b::tstzrange)::int +
     allen_preceded_by(a::tstzrange, b::tstzrange)::int
    ) AS true_count
FROM (VALUES
    ('[2000-01-01,2000-04-01)', '[2000-07-01,2000-10-01)'),
    ('[2000-01-01,2000-04-01)', '[2000-04-01,2000-07-01)'),
    ('[2000-01-01,2000-04-01)', '[2000-03-01,2000-06-01)'),
    ('[2000-01-01,2000-04-01)', '[2000-02-01,2000-04-01)'),
    ('[2000-01-01,2000-10-01)', '[2000-03-01,2000-06-01)'),
    ('[2000-01-01,2000-04-01)', '[2000-01-01,2000-06-01)'),
    ('[2000-01-01,2000-04-01)', '[2000-01-01,2000-04-01)'),
    ('[2000-01-01,2000-06-01)', '[2000-01-01,2000-04-01)'),
    ('[2000-03-01,2000-06-01)', '[2000-01-01,2000-10-01)'),
    ('[2000-02-01,2000-04-01)', '[2000-01-01,2000-04-01)'),
    ('[2000-03-01,2000-06-01)', '[2000-01-01,2000-04-01)'),
    ('[2000-04-01,2000-07-01)', '[2000-01-01,2000-04-01)'),
    ('[2000-07-01,2000-10-01)', '[2000-01-01,2000-04-01)')
) AS t(a, b);

-- ============================================================
-- Infinite bounds
-- ============================================================
\echo '-- infinite bounds'
-- [2000,∞) contains [Jun,Sep)
SELECT allen_contains('[2000-01-01,)'::tstzrange,
                      '[2000-06-01,2000-09-01)'::tstzrange);
-- (-∞,Apr) precedes [Jul,∞)
SELECT allen_precedes('(,2000-04-01)'::tstzrange,
                      '[2000-07-01,)'::tstzrange);
-- (-∞,∞) contains any finite range
SELECT allen_contains('(,)'::tstzrange,
                      '[2000-01-01,2000-04-01)'::tstzrange);
-- (-∞,∞) equals itself
SELECT allen_equals('(,)'::tstzrange, '(,)'::tstzrange);
-- [2000,∞) meets nothing (upper is +inf)
SELECT allen_meets('[2000-01-01,)'::tstzrange,
                   '[2000-04-01,)'::tstzrange);

-- ============================================================
-- Other range types (int4range)
-- ============================================================
\echo '-- int4range'
SELECT allen_precedes('[1,5)'::int4range, '[10,20)'::int4range);
SELECT allen_overlaps('[1,10)'::int4range, '[5,15)'::int4range);
SELECT allen_during('[5,10)'::int4range, '[1,20)'::int4range);
SELECT allen_relation('[5,10)'::int4range, '[1,20)'::int4range);
SELECT allen_contains('[1,100)'::int4range, '[10,20)'::int4range);

-- ============================================================
-- Cross-check: allen_any_overlap must agree with built-in &&
-- Relations that imply &&: o, Fi, Di, s, e, Si, d, f, Oi
-- Relations that do NOT imply &&: p, m, Mi, Pi
-- ============================================================
\echo '-- cross-check && vs allen_any_overlap'
SELECT
    (a::tstzrange && b::tstzrange) = allen_any_overlap(a::tstzrange, b::tstzrange) AS match
FROM (VALUES
    ('[2000-01-01,2000-04-01)', '[2000-07-01,2000-10-01)'),  -- p  -> both false
    ('[2000-01-01,2000-04-01)', '[2000-04-01,2000-07-01)'),  -- m  -> both false
    ('[2000-01-01,2000-04-01)', '[2000-03-01,2000-06-01)'),  -- o  -> both true
    ('[2000-01-01,2000-04-01)', '[2000-02-01,2000-04-01)'),  -- Fi -> both true
    ('[2000-01-01,2000-10-01)', '[2000-03-01,2000-06-01)'),  -- Di -> both true
    ('[2000-01-01,2000-04-01)', '[2000-01-01,2000-06-01)'),  -- s  -> both true
    ('[2000-01-01,2000-04-01)', '[2000-01-01,2000-04-01)'),  -- e  -> both true
    ('[2000-01-01,2000-06-01)', '[2000-01-01,2000-04-01)'),  -- Si -> both true
    ('[2000-03-01,2000-06-01)', '[2000-01-01,2000-10-01)'),  -- d  -> both true
    ('[2000-02-01,2000-04-01)', '[2000-01-01,2000-04-01)'),  -- f  -> both true
    ('[2000-03-01,2000-06-01)', '[2000-01-01,2000-04-01)'),  -- Oi -> both true
    ('[2000-04-01,2000-07-01)', '[2000-01-01,2000-04-01)'),  -- Mi -> both false
    ('[2000-07-01,2000-10-01)', '[2000-01-01,2000-04-01)')   -- Pi -> both false
) AS t(a, b);

-- ============================================================
-- NULL / empty range handling: all functions return false/NULL
-- ============================================================
\echo '-- empty ranges'
SELECT allen_precedes('empty'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);
SELECT allen_during('empty'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange);
SELECT allen_relation('empty'::tstzrange, '[2000-01-01,2000-04-01)'::tstzrange) IS NULL AS is_null;
