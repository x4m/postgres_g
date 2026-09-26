/*-------------------------------------------------------------------------
 *
 * test_page_history.c
 *      Bounded, volatile page history collected from ordinary standby redo.
 *
 * This is a correctness scaffold, not a durable storage engine.  Selected
 * main forks are retained from a paused, consistent baseline.  The startup
 * process then appends images after each complete WAL record.  Readers can
 * only use published record boundaries; unpublished images are invisible.  Neither
 * capacity exhaustion nor an unsupported record permits a latest-page
 * fallback.  Restart revokes all views instead of guessing their contents.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

PG_FUNCTION_INFO_V1(test_page_store_retain);
PG_FUNCTION_INFO_V1(test_page_store_retain_relations);
PG_FUNCTION_INFO_V1(test_page_store_history_status);

#define HISTORY_MAX_RECORDS 65536
#define HISTORY_MAX_RELATIONS 16

typedef struct HistoryPage
{
	uint32		relation;		/* index in the immutable locator registry */
	BlockNumber block;
	PGAlignedBlock image;
} HistoryPage;

typedef struct HistoryFile
{
	BlockNumber nblocks;
	uint32		floor;			/* first image of this file incarnation */
	bool		exists;
} HistoryFile;

typedef struct HistoryRecord
{
	XLogRecPtr	lsn;
	uint32		nimages;		/* cumulative count of published images */
	HistoryFile files[HISTORY_MAX_RELATIONS];
} HistoryRecord;

typedef struct HistoryControl
{
	LWLock		lock;
	ConditionVariable changed;
	RelFileLocator locators[HISTORY_MAX_RELATIONS];
	uint32		nrelations;		/* immutable after baseline publication */
	TimeLineID	tli;
	uint32		nrecords;		/* zero until the baseline is published */
	bool		stopped;
	char		reason[128];
} HistoryControl;

static int	history_capacity;
static HistoryControl *history;
static HistoryPage *history_pages;
static HistoryRecord *history_records;
static after_wal_replay_hook_type previous_replay_hook;

static void history_request(void *arg);
static void history_initialize(void *arg);
static void history_replay(XLogReaderState *record, TimeLineID tli);

static const ShmemCallbacks history_callbacks = {
	.request_fn = history_request,
	.init_fn = history_initialize,
};

void
test_page_store_history_init(void)
{
	DefineCustomIntVariable("test_page_store.history_pages",
							"Maximum retained main-fork page images.",
							NULL, &history_capacity, 0, 0, 131072,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	RegisterShmemCallbacks(&history_callbacks);
	previous_replay_hook = after_wal_replay_hook;
	after_wal_replay_hook = history_replay;
}

static void
history_request(void *arg)
{
	if (history_capacity == 0)
		return;
	ShmemRequestStruct(.name = "test_page_store history",
					   .size = sizeof(HistoryControl),
					   .ptr = (void **) &history);
	ShmemRequestStruct(.name = "test_page_store images",
					   .size = mul_size(history_capacity, sizeof(HistoryPage)),
					   .ptr = (void **) &history_pages);
	ShmemRequestStruct(.name = "test_page_store records",
					   .size = mul_size(HISTORY_MAX_RECORDS, sizeof(HistoryRecord)),
					   .ptr = (void **) &history_records);
}

static void
history_initialize(void *arg)
{
	if (!history)
		return;
	memset(history, 0, sizeof(*history));
	LWLockInitialize(&history->lock,
					 LWLockNewTrancheId("test_page_store history"));
	ConditionVariableInit(&history->changed);
}

bool
test_page_store_history_enabled(void)
{
	return history_capacity > 0;
}

/* The destination must not be in any published record's image range. */
static void
history_copy_page(RelFileLocator locator, uint32 relation,
				  BlockNumber block, uint32 dest)
{
	Buffer		buffer;
	HistoryPage *page = &history_pages[dest];

	Assert(dest < history_capacity);
	buffer = ReadBufferWithoutRelcache(locator, MAIN_FORKNUM, block,
									   RBM_NORMAL, NULL, true);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page->relation = relation;
	page->block = block;
	memcpy(page->image.data, BufferGetPage(buffer), BLCKSZ);
	UnlockReleaseBuffer(buffer);
	PageSetChecksum(page->image.data, block);
	if (AmStartupProcess())
		INJECTION_POINT("test-page-store-after-history-page", NULL);
}

static XLogRecPtr
history_retain(const Oid *oids, int nrelations)
{
	Relation	rels[HISTORY_MAX_RELATIONS];
	HistoryRecord baseline = {0};
	TimeLineID	tli;
	XLogRecPtr	lsn;
	uint64		nimages = 0;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use page service prototype")));
	if (!history)
		ereport(ERROR, (errmsg("page history requires shared preload and history_pages > 0")));
	if (nrelations < 1 || nrelations > HISTORY_MAX_RELATIONS)
		ereport(ERROR,
				(errmsg("page history requires between 1 and %d relations",
						HISTORY_MAX_RELATIONS)));
	lsn = GetXLogReplayRecPtr(&tli);
	test_page_store_check_cut(tli, lsn);
	baseline.lsn = lsn;
	for (int i = 0; i < nrelations; i++)
	{
		rels[i] = relation_open(oids[i], AccessShareLock);
		if (!RELKIND_HAS_STORAGE(rels[i]->rd_rel->relkind) ||
			rels[i]->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
			ereport(ERROR, (errmsg("page history requires a permanent stored relation")));
		for (int j = 0; j < i; j++)
			if (RelFileLocatorEquals(rels[i]->rd_locator, rels[j]->rd_locator))
				ereport(ERROR, (errmsg("duplicate relation in page history baseline")));
		baseline.files[i].exists = true;
		baseline.files[i].nblocks = RelationGetNumberOfBlocks(rels[i]);
		nimages += baseline.files[i].nblocks;
		if (nimages > history_capacity)
			ereport(ERROR, (errmsg("page history capacity is smaller than the baseline")));
	}

	LWLockAcquire(&history->lock, LW_EXCLUSIVE);
	if (history->nrecords != 0)
	{
		LWLockRelease(&history->lock);
		ereport(ERROR, (errmsg("page history is already initialized")));
	}
	for (int i = 0; i < nrelations; i++)
	{
		for (BlockNumber block = 0; block < baseline.files[i].nblocks; block++)
		{
			history_copy_page(rels[i]->rd_locator, i, block, baseline.nimages++);
			CHECK_FOR_INTERRUPTS();
		}
	}

	/*
	 * A resumed startup might have changed pages without publishing its
	 * replay position yet.  Check the in-flight position as well.  Its
	 * after-record callback takes our lock even when history is inactive, so
	 * it cannot miss activation between this check and publication.
	 */
	test_page_store_check_cut(tli, lsn);
	if (GetCurrentReplayRecPtr(NULL) != lsn)
		ereport(ERROR, (errmsg("recovery moved while taking the history baseline")));
	for (int i = 0; i < nrelations; i++)
		history->locators[i] = rels[i]->rd_locator;
	history->nrelations = nrelations;
	history->tli = tli;
	history_records[0] = baseline;
	history->nrecords = 1;
	LWLockRelease(&history->lock);
	for (int i = 0; i < nrelations; i++)
		relation_close(rels[i], AccessShareLock);
	ConditionVariableBroadcast(&history->changed);
	return lsn;
}

Datum
test_page_store_retain(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	PG_RETURN_LSN(history_retain(&oid, 1));
}

Datum
test_page_store_retain_relations(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *values;
	bool	   *nulls;
	int			count;
	Oid			oids[HISTORY_MAX_RELATIONS];
	XLogRecPtr	lsn;

	count = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	if (count < 1 || count > HISTORY_MAX_RELATIONS)
		ereport(ERROR,
				(errmsg("page history requires between 1 and %d relations",
						HISTORY_MAX_RELATIONS)));
	deconstruct_array(array, REGCLASSOID, sizeof(Oid), true, TYPALIGN_INT,
					  &values, &nulls, &count);
	for (int i = 0; i < count; i++)
	{
		if (nulls[i])
			ereport(ERROR, (errmsg("null relation in page history baseline")));
		oids[i] = DatumGetObjectId(values[i]);
	}
	lsn = history_retain(oids, count);
	pfree(values);
	pfree(nulls);
	PG_FREE_IF_COPY(array, 0);
	PG_RETURN_LSN(lsn);
}

static void
history_stop(const char *reason)
{
	LWLockAcquire(&history->lock, LW_EXCLUSIVE);
	history->stopped = true;
	strlcpy(history->reason, reason, sizeof(history->reason));
	LWLockRelease(&history->lock);
	ConditionVariableBroadcast(&history->changed);
}

/*
 * Relation deletion is logical at this record, even if md keeps an empty
 * segment around until checkpoint.  File existence alone is not an oracle.
 * Return false for an unrecognized special record, to stop collecting.
 */
static bool
history_special(XLogReaderState *record, RelFileLocator locator,
				HistoryFile *next, uint32 nimages, bool *refresh)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	RelFileLocator *dropped = NULL;
	int			ndropped = 0;

	switch (XLogRecGetRmid(record))
	{
		case RM_SMGR_ID:
			if (info == XLOG_SMGR_CREATE)
			{
				xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);

				if (RelFileLocatorEquals(locator, xlrec->rlocator) &&
					xlrec->forkNum == MAIN_FORKNUM)
				{
					next->floor = nimages;
					next->exists = true;
					*refresh = true;
				}
			}
			else if (info == XLOG_SMGR_TRUNCATE)
			{
				xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);

				if (RelFileLocatorEquals(locator, xlrec->rlocator) &&
					(xlrec->flags & SMGR_TRUNCATE_HEAP))
					next->nblocks = xlrec->blkno;
			}
			else
				return false;
			break;
		case RM_XACT_ID:
			info &= XLOG_XACT_OPMASK;
			if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
			{
				xl_xact_parsed_commit parsed;

				ParseCommitRecord(XLogRecGetInfo(record),
								  (xl_xact_commit *) XLogRecGetData(record), &parsed);
				ndropped = parsed.nrels;
				dropped = parsed.xlocators;
			}
			else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
			{
				xl_xact_parsed_abort parsed;

				ParseAbortRecord(XLogRecGetInfo(record),
								 (xl_xact_abort *) XLogRecGetData(record), &parsed);
				ndropped = parsed.nrels;
				dropped = parsed.xlocators;
			}
			else
				return false;
			for (int i = 0; i < ndropped; i++)
				if (RelFileLocatorEquals(locator, dropped[i]))
				{
					next->exists = false;
					next->nblocks = 0;
				}
			break;
		case RM_DBASE_ID:
			if (info == XLOG_DBASE_DROP)
			{
				xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *) XLogRecGetData(record);

				if (xlrec->db_id == locator.dbOid)
					for (int i = 0; i < xlrec->ntablespaces; i++)
						if (xlrec->tablespace_ids[i] == locator.spcOid)
						{
							next->exists = false;
							next->nblocks = 0;
						}
			}
			else if (info == XLOG_DBASE_CREATE_FILE_COPY)
			{
				xl_dbase_create_file_copy_rec *xlrec =
					(xl_dbase_create_file_copy_rec *) XLogRecGetData(record);

				if (xlrec->db_id == locator.dbOid &&
					xlrec->tablespace_id == locator.spcOid)
				{
					next->floor = nimages;
					*refresh = true;
				}
			}
			else
				return false;
			break;
		default:
			return false;
	}
	return true;
}

/* Append one file's images, without publishing any part of the record. */
static bool
history_replay_file(XLogReaderState *record, uint32 relation, HistoryRecord *next)
{
	RelFileLocator locator = history->locators[relation];
	HistoryFile *file = &next->files[relation];
	BlockNumber blocks[XLR_MAX_BLOCK_ID + 1];
	BlockNumber copy_from;
	int			nblocks = 0;
	bool		refresh = false;

	if ((XLogRecGetInfo(record) & XLR_SPECIAL_REL_UPDATE) &&
		!history_special(record, locator, file, next->nimages, &refresh))
	{
		history_stop("unsupported special relation update");
		return false;
	}
	for (int i = 0; i <= XLogRecMaxBlockId(record); i++)
	{
		RelFileLocator rlocator;
		ForkNumber	forknum;
		BlockNumber block;

		if (!XLogRecHasBlockRef(record, i))
			continue;
		XLogRecGetBlockTag(record, i, &rlocator, &forknum, &block);
		if (forknum == MAIN_FORKNUM && RelFileLocatorEquals(locator, rlocator))
			blocks[nblocks++] = block;
	}
	copy_from = file->nblocks;
	if (refresh || nblocks > 0)
	{
		SMgrRelation smgr = smgropen(locator, INVALID_PROC_NUMBER);

		if (refresh)
		{
			file->exists = smgrexists(smgr, MAIN_FORKNUM);
			copy_from = 0;
		}
		if (!file->exists && nblocks > 0)
		{
			history_stop("block reference to an absent retained relation");
			return false;
		}
		file->nblocks = file->exists ? smgrnblocks(smgr, MAIN_FORKNUM) : 0;
		if (copy_from > file->nblocks)
		{
			history_stop("unexpected main-fork shrink");
			return false;
		}
	}

	/*
	 * Copy newly extended blocks, including any intervening zero pages.
	 * Overestimate duplicate references rather than risk a partial record.
	 */
	if ((uint64) next->nimages + (file->nblocks - copy_from) + nblocks > history_capacity)
	{
		history_stop("page capacity exhausted");
		return false;
	}
	for (BlockNumber block = copy_from; block < file->nblocks; block++)
		history_copy_page(locator, relation, block, next->nimages++);
	for (int i = 0; i < nblocks; i++)
	{
		if (blocks[i] >= file->nblocks)
		{
			history_stop("block reference beyond the retained relation");
			return false;
		}
		if (blocks[i] < copy_from)
			history_copy_page(locator, relation, blocks[i], next->nimages++);
	}
	return true;
}

static void
history_replay(XLogReaderState *record, TimeLineID tli)
{
	HistoryRecord next;
	uint32		nrecords;

	if (previous_replay_hook)
		previous_replay_hook(record, tli);
	if (!history)
		return;
	LWLockAcquire(&history->lock, LW_SHARED);
	if (history->nrecords == 0 || history->stopped)
	{
		LWLockRelease(&history->lock);
		return;
	}
	nrecords = history->nrecords;
	next = history_records[nrecords - 1];
	LWLockRelease(&history->lock);
	if (tli != history->tli)
	{
		history_stop("timeline changed");
		return;
	}
	if (nrecords == HISTORY_MAX_RECORDS)
	{
		history_stop("record capacity exhausted");
		return;
	}
	Assert(record->EndRecPtr > next.lsn);
	next.lsn = record->EndRecPtr;
	for (uint32 i = 0; i < history->nrelations; i++)
	{
		if (!history_replay_file(record, i, &next))
			return;
		if (history_records[nrecords - 1].files[i].exists &&
			!next.files[i].exists)
			INJECTION_POINT("test-page-store-after-history-drop", NULL);
	}

	/* All files' images and lifecycle changes become visible together. */
	LWLockAcquire(&history->lock, LW_EXCLUSIVE);
	history_records[nrecords] = next;
	history->nrecords = nrecords + 1;
	LWLockRelease(&history->lock);
	ConditionVariableBroadcast(&history->changed);
}

/* A following compute may reach a record before this storage has applied it. */
static void
history_wait_for_replay(XLogRecPtr lsn)
{
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);
	uint32		wait_event = WaitEventExtensionNew("TestPageStoreHistory");

	ConditionVariablePrepareToSleep(&history->changed);
	for (;;)
	{
		bool		ready;
		long		remaining;

		LWLockAcquire(&history->lock, LW_SHARED);
		ready = history->nrecords == 0 || history->stopped ||
			lsn <= history_records[history->nrecords - 1].lsn;
		LWLockRelease(&history->lock);
		if (ready)
			break;
		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remaining <= 0)
		{
			ConditionVariableCancelSleep();
			ereport(ERROR, (errmsg("timed out waiting for retained page history")));
		}
		ConditionVariableTimedSleep(&history->changed, remaining, wait_event);
	}
	ConditionVariableCancelSleep();
}

bytea *
test_page_store_history_fetch(RelFileLocator locator, ForkNumber forknum,
							  BlockNumber block, int count, TimeLineID tli,
							  XLogRecPtr lsn, bool wait, bool *exists, BlockNumber *nblocks)
{
	HistoryRecord record;
	HistoryFile *file;
	bytea	   *pages = palloc(VARHDRSZ + count * BLCKSZ);
	uint32		low = 0;
	uint32		high;
	uint32		relation;

	SET_VARSIZE(pages, VARHDRSZ + count * BLCKSZ);
	if (!RecoveryInProgress())
		ereport(ERROR, (errmsg("page service requires a standby")));
	if (!history)
		ereport(ERROR, (errmsg("page history is not available")));
	if (wait)
		history_wait_for_replay(lsn);
	LWLockAcquire(&history->lock, LW_SHARED);
	if (history->nrecords == 0)
		ereport(ERROR, (errmsg("page history is not initialized")));
	for (relation = 0; relation < history->nrelations; relation++)
		if (RelFileLocatorEquals(locator, history->locators[relation]))
			break;
	if (tli != history->tli || relation == history->nrelations ||
		forknum != MAIN_FORKNUM)
		ereport(ERROR, (errmsg("requested relation, fork, or timeline is not retained")));
	high = history->nrecords;
	while (low < high)
	{
		uint32		mid = low + (high - low) / 2;

		if (history_records[mid].lsn < lsn)
			low = mid + 1;
		else
			high = mid;
	}
	if (low == history->nrecords || history_records[low].lsn != lsn)
		ereport(ERROR,
				(errmsg("requested record boundary is not retained"),
				 errdetail("History ends at %X/%X.%s%s",
						   LSN_FORMAT_ARGS(history_records[history->nrecords - 1].lsn),
						   history->stopped ? " Collection stopped: " : "",
						   history->reason)));
	record = history_records[low];
	file = &record.files[relation];
	*exists = file->exists;
	*nblocks = file->nblocks;
	if (count > 0 && (!file->exists || (uint64) block + count > file->nblocks))
		ereport(ERROR, (errmsg("requested blocks are outside the retained relation")));
	for (int i = 0; i < count; i++)
	{
		bool		found = false;

		for (uint32 j = record.nimages; j > file->floor; j--)
			if (history_pages[j - 1].relation == relation &&
				history_pages[j - 1].block == block + i)
			{
				memcpy(VARDATA(pages) + i * BLCKSZ,
					   history_pages[j - 1].image.data, BLCKSZ);
				found = true;
				break;
			}
		if (!found)
			elog(ERROR, "missing page in retained history");
	}
	LWLockRelease(&history->lock);
	return pages;
}

Datum
test_page_store_history_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[8];
	bool		nulls[8] = {false};
	HistoryRecord first = {0};
	HistoryRecord last = {0};
	TimeLineID	tli;
	uint32		nrecords;
	bool		stopped;
	char		reason[sizeof(history->reason)];

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use page service prototype")));
	if (!history)
		ereport(ERROR, (errmsg("page history is not available")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	LWLockAcquire(&history->lock, LW_SHARED);
	nrecords = history->nrecords;
	if (nrecords > 0)
	{
		first = history_records[0];
		last = history_records[nrecords - 1];
	}
	tli = history->tli;
	stopped = history->stopped;
	memcpy(reason, history->reason, sizeof(reason));
	LWLockRelease(&history->lock);
	if (nrecords == 0)
		nulls[0] = nulls[1] = nulls[2] = true;
	values[0] = LSNGetDatum(first.lsn);
	values[1] = LSNGetDatum(last.lsn);
	values[2] = Int64GetDatum(tli);
	values[3] = Int32GetDatum(nrecords ? last.nimages : 0);
	values[4] = Int32GetDatum(nrecords);
	values[5] = BoolGetDatum(nrecords > 0 && !stopped);
	values[6] = CStringGetTextDatum(reason);
	values[7] = LSNGetDatum(GetCurrentReplayRecPtr(NULL));
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
