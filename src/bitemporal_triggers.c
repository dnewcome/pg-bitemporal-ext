/*
 * bitemporal_triggers.c
 *
 * System-time management trigger for bitemporal tables.
 * Install with bitemporal.enable() — do not call directly.
 *
 * Trigger function: bitemporal.systime_trigger()
 * Fire:  BEFORE INSERT OR UPDATE OR DELETE FOR EACH ROW
 *
 * Trigger arguments (TG_ARGV):
 *   [0]  sys_start_col   column name holding transaction-time start
 *   [1]  sys_end_col     column name holding transaction-time end
 *   [2]  mode            'system_only' or 'full' (informational, unused here)
 *   [3+] key_col(s)      one argument per business-key column
 *
 * Semantics (depth 0 only — depth > 1 passes through):
 *
 *   INSERT
 *     1. Close any existing current row for the same key:
 *           UPDATE t SET sys_end = now WHERE key = NEW.key AND sys_end = ∞
 *     2. Overwrite sys_start = now, sys_end = ∞ on the incoming tuple.
 *     3. Return the modified tuple → INSERT proceeds.
 *
 *   UPDATE
 *     1. Close the current row (identified by OLD key).
 *     2. INSERT a new version carrying NEW data with fresh temporal columns.
 *     3. Return NULL → original UPDATE is cancelled.
 *
 *   DELETE
 *     1. Close the current row (identified by OLD key).
 *     2. Return NULL → physical DELETE is cancelled (soft-delete via sys_end).
 *
 * Key columns must be NOT NULL. The EXCLUDE constraint installed by
 * bitemporal.enable() enforces temporal uniqueness.
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(bitemporal_systime_trigger);

/*
 * Recursion guard: incremented while we are inside the trigger's SPI
 * operations so that the re-entrant calls (from close_current_row's UPDATE
 * and insert_new_version's INSERT) pass through without re-processing.
 *
 * Wrapped in PG_TRY/PG_CATCH below so it is always decremented even when
 * an elog(ERROR) aborts the transaction, preventing stale state across
 * transaction boundaries.
 */
static int bitemporal_nesting = 0;

/* ----------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------- */

/*
 * close_current_row
 *
 * Executes:
 *   UPDATE <schema>.<table> SET <sys_end> = $1
 *   WHERE  <key1> = $2 [AND <key2> = $3 ...] AND <sys_end> = 'infinity'
 *
 * Sets sys_end to close_ts on the currently-current row(s) for this key.
 * It is not an error if no rows match (e.g. first INSERT for a new key).
 *
 * Caller must have called SPI_connect() before this function.
 */
static void
close_current_row(Relation rel, TupleDesc tupdesc,
				  const char *sys_end_col,
				  char **key_cols, int nkeys, const int *key_attnos,
				  HeapTuple key_tuple, TimestampTz close_ts)
{
	StringInfoData	buf;
	Datum		   *params;
	Oid			   *param_types;
	char		   *nulls;
	int				nparams = nkeys + 1;
	int				ret;
	int				i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "UPDATE %s.%s SET %s = $1 WHERE ",
					 quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
					 quote_identifier(RelationGetRelationName(rel)),
					 quote_identifier(sys_end_col));
	for (i = 0; i < nkeys; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "%s = $%d", quote_identifier(key_cols[i]), i + 2);
	}
	appendStringInfo(&buf, " AND %s = 'infinity'::timestamptz",
					 quote_identifier(sys_end_col));

	params		= palloc(nparams * sizeof(Datum));
	param_types	= palloc(nparams * sizeof(Oid));
	nulls		= palloc(nparams + 1);
	nulls[nparams] = '\0';

	/* $1: close_ts */
	params[0]		= TimestampTzGetDatum(close_ts);
	param_types[0]	= TIMESTAMPTZOID;
	nulls[0]		= ' ';

	/* $2..n: key values from key_tuple */
	for (i = 0; i < nkeys; i++)
	{
		bool isnull;

		params[i + 1] = heap_getattr(key_tuple, key_attnos[i], tupdesc, &isnull);
		param_types[i + 1] = TupleDescAttr(tupdesc, key_attnos[i] - 1)->atttypid;
		nulls[i + 1] = isnull ? 'n' : ' ';
	}

	ret = SPI_execute_with_args(buf.data, nparams, param_types, params, nulls,
								false, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "bitemporal_systime_trigger: failed to close current row");
}

/*
 * insert_new_version
 *
 * Builds and executes an INSERT that copies all non-dropped, non-generated
 * columns from newtuple but overrides the temporal columns:
 *   sys_start = sys_start_val
 *   sys_end   = +infinity
 *
 * Generated columns (sys_range, valid_range) are intentionally skipped —
 * the executor will recompute them after the INSERT.
 *
 * Caller must have called SPI_connect() before this function.
 */
static void
insert_new_version(Relation rel, TupleDesc tupdesc,
				   HeapTuple newtuple,
				   int sys_start_attno, int sys_end_attno,
				   TimestampTz sys_start_val)
{
	StringInfoData	cols_buf,
					vals_buf,
					query_buf;
	int				natts = tupdesc->natts;
	Datum		   *params;
	Oid			   *param_types;
	char		   *nulls;
	int				nparams = 0;
	int				pidx;
	int				ret;
	int				i;
	bool			first = true;
	TimestampTz		inf;

	TIMESTAMP_NOEND(inf);

	/* Count insertable columns (not dropped, not generated) */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!attr->attisdropped && attr->attgenerated == '\0')
			nparams++;
	}

	params		= palloc(nparams * sizeof(Datum));
	param_types	= palloc(nparams * sizeof(Oid));
	nulls		= palloc(nparams + 1);
	nulls[nparams] = '\0';

	initStringInfo(&cols_buf);
	initStringInfo(&vals_buf);

	appendStringInfo(&cols_buf, "INSERT INTO %s.%s (",
					 quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
					 quote_identifier(RelationGetRelationName(rel)));

	pidx = 0;
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		int attno = i + 1;

		if (attr->attisdropped || attr->attgenerated != '\0')
			continue;

		if (!first)
		{
			appendStringInfoChar(&cols_buf, ',');
			appendStringInfoChar(&vals_buf, ',');
		}
		first = false;

		appendStringInfoString(&cols_buf, quote_identifier(NameStr(attr->attname)));
		appendStringInfo(&vals_buf, "$%d", pidx + 1);

		if (attno == sys_start_attno)
		{
			params[pidx]		= TimestampTzGetDatum(sys_start_val);
			param_types[pidx]	= TIMESTAMPTZOID;
			nulls[pidx]			= ' ';
		}
		else if (attno == sys_end_attno)
		{
			params[pidx]		= TimestampTzGetDatum(inf);
			param_types[pidx]	= TIMESTAMPTZOID;
			nulls[pidx]			= ' ';
		}
		else
		{
			bool isnull;

			params[pidx]		= heap_getattr(newtuple, attno, tupdesc, &isnull);
			param_types[pidx]	= attr->atttypid;
			nulls[pidx]			= isnull ? 'n' : ' ';
		}
		pidx++;
	}

	appendStringInfoChar(&cols_buf, ')');

	initStringInfo(&query_buf);
	appendStringInfo(&query_buf, "%s VALUES (%s)", cols_buf.data, vals_buf.data);

	ret = SPI_execute_with_args(query_buf.data, nparams, param_types, params, nulls,
								false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "bitemporal_systime_trigger: INSERT of new version failed");
}

/* ----------------------------------------------------------------
 * Main trigger function
 * ---------------------------------------------------------------- */

Datum
bitemporal_systime_trigger(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata;
	Trigger		   *trigger;
	Relation		rel;
	TupleDesc		tupdesc;
	TriggerEvent	event;
	HeapTuple		rettuple = NULL;
	TimestampTz		now_ts;
	char		  **argv;
	int				nargs;
	const char	   *sys_start_col;
	const char	   *sys_end_col;
	int				nkeys;
	char		  **key_cols;
	int			   *key_attnos;
	int				sys_start_attno,
					sys_end_attno;
	int				i;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "bitemporal_systime_trigger: not called as trigger");

	trigdata = (TriggerData *) fcinfo->context;
	trigger  = trigdata->tg_trigger;
	rel		 = trigdata->tg_relation;
	tupdesc  = rel->rd_att;
	event	 = trigdata->tg_event;

	/*
	 * Recursion guard.  bitemporal_nesting > 0 means we're being called
	 * from within our own SPI operations (close_current_row UPDATE or
	 * insert_new_version INSERT).  Pass through without processing to
	 * avoid infinite loops.
	 */
	if (bitemporal_nesting > 0)
	{
		if (TRIGGER_FIRED_BY_INSERT(event))
			return PointerGetDatum(trigdata->tg_trigtuple);
		else if (TRIGGER_FIRED_BY_UPDATE(event))
			return PointerGetDatum(trigdata->tg_newtuple);
		else
			return PointerGetDatum(trigdata->tg_trigtuple);
	}

	/* Validate trigger arguments */
	nargs = trigger->tgnargs;
	argv  = trigger->tgargs;

	if (nargs < 4)
		elog(ERROR,
			 "bitemporal_systime_trigger: requires ≥4 arguments: "
			 "sys_start_col, sys_end_col, mode, key_col [...]");

	sys_start_col = argv[0];
	sys_end_col   = argv[1];
	/* argv[2] = mode — informational, not needed inside the trigger */
	nkeys     = nargs - 3;
	key_cols  = &argv[3];

	/* Resolve attribute numbers (1-based) */
	sys_start_attno = SPI_fnumber(tupdesc, sys_start_col);
	sys_end_attno   = SPI_fnumber(tupdesc, sys_end_col);

	if (sys_start_attno == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "bitemporal_systime_trigger: sys_start column \"%s\" not found",
			 sys_start_col);
	if (sys_end_attno == SPI_ERROR_NOATTRIBUTE)
		elog(ERROR, "bitemporal_systime_trigger: sys_end column \"%s\" not found",
			 sys_end_col);

	key_attnos = palloc(nkeys * sizeof(int));
	for (i = 0; i < nkeys; i++)
	{
		key_attnos[i] = SPI_fnumber(tupdesc, key_cols[i]);
		if (key_attnos[i] == SPI_ERROR_NOATTRIBUTE)
			elog(ERROR,
				 "bitemporal_systime_trigger: key column \"%s\" not found",
				 key_cols[i]);
	}

	/*
	 * Use transaction start time as the system-time boundary so that
	 * all mutations in a single transaction share the same timestamp.
	 */
	now_ts = GetCurrentTransactionStartTimestamp();

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "bitemporal_systime_trigger: SPI_connect failed");

	/*
	 * Increment nesting counter inside PG_TRY so it is always decremented
	 * even when an elog(ERROR) propagates out.  This prevents stale state
	 * if a constraint violation or other error aborts the transaction.
	 */
	bitemporal_nesting++;
	PG_TRY();
	{
		/* ---- INSERT ----------------------------------------------- */
		if (TRIGGER_FIRED_BY_INSERT(event))
		{
			HeapTuple	newtuple = trigdata->tg_trigtuple;
			int			natts = tupdesc->natts;
			Datum	   *replVals  = palloc0(natts * sizeof(Datum));
			bool	   *replIsNull = palloc0(natts * sizeof(bool));
			bool	   *replRepl   = palloc0(natts * sizeof(bool));
			TimestampTz	inf;

			TIMESTAMP_NOEND(inf);

			/* Step 1: close any existing current row for this key */
			close_current_row(rel, tupdesc, sys_end_col, key_cols, nkeys, key_attnos,
							  newtuple, now_ts);

			/* Step 2: stamp sys_start = now, sys_end = ∞ on the incoming tuple */
			replRepl[sys_start_attno - 1]   = true;
			replVals[sys_start_attno - 1]   = TimestampTzGetDatum(now_ts);
			replIsNull[sys_start_attno - 1] = false;

			replRepl[sys_end_attno - 1]     = true;
			replVals[sys_end_attno - 1]     = TimestampTzGetDatum(inf);
			replIsNull[sys_end_attno - 1]   = false;

			rettuple = heap_modify_tuple(newtuple, tupdesc,
										 replVals, replIsNull, replRepl);
		}
		/* ---- UPDATE ----------------------------------------------- */
		else if (TRIGGER_FIRED_BY_UPDATE(event))
		{
			HeapTuple	oldtuple = trigdata->tg_trigtuple;
			HeapTuple	newtuple = trigdata->tg_newtuple;

			/* Step 1: close the current row (keyed by OLD values) */
			close_current_row(rel, tupdesc, sys_end_col, key_cols, nkeys, key_attnos,
							  oldtuple, now_ts);

			/* Step 2: insert a new version carrying the NEW data */
			insert_new_version(rel, tupdesc, newtuple,
							   sys_start_attno, sys_end_attno, now_ts);

			/* rettuple stays NULL → original UPDATE is cancelled */
		}
		/* ---- DELETE ----------------------------------------------- */
		else
		{
			HeapTuple	oldtuple = trigdata->tg_trigtuple;

			/* Close the current row (soft delete) */
			close_current_row(rel, tupdesc, sys_end_col, key_cols, nkeys, key_attnos,
							  oldtuple, now_ts);

			/* rettuple stays NULL → physical DELETE is cancelled */
		}
	}
	PG_CATCH();
	{
		bitemporal_nesting--;
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	bitemporal_nesting--;
	SPI_finish();
	return PointerGetDatum(rettuple);
}
