/*-------------------------------------------------------------------------
 *
 * test_page_history.c
 *      Bounded page history collected from ordinary standby redo.
 *
 * This is a correctness scaffold, not a complete storage engine.  Selected
 * main and visibility map forks are retained from a paused baseline.  Startup
 * then appends images after each complete WAL record.  Readers can
 * only use published record boundaries; unpublished images are invisible.
 * Neither capacity exhaustion nor an unsupported record permits a latest-page
 * fallback.  History is volatile by default; an optional journal persists
 * complete records before publication and reloads them after restart.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

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
#include "port/pg_crc32c.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
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
#define HISTORY_JOURNAL "test_page_store.history"
#define HISTORY_JOURNAL_TEMP HISTORY_JOURNAL ".tmp"
#define HISTORY_JOURNAL_MAGIC 0x50475348
#define HISTORY_JOURNAL_VERSION 2

typedef struct HistoryPage
{
	uint32		relation;		/* index in the immutable locator registry */
	ForkNumber	forknum;
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
	HistoryFile files[HISTORY_MAX_RELATIONS][MAX_FORKNUM + 1];
} HistoryRecord;

/* Local prototype format, not a portable storage or wire protocol. */
typedef struct HistoryJournalHeader
{
	uint32		magic;
	uint32		version;
	uint32		pg_version;
	uint32		block_size;
	uint32		record_size;
	uint32		image_size;
	uint64		system_identifier;
	uint32		nrelations;
	TimeLineID	tli;
	RelFileLocator locators[HISTORY_MAX_RELATIONS];
	pg_crc32c	crc;
} HistoryJournalHeader;

typedef struct HistoryJournalFrame
{
	uint32		magic;
	uint32		sequence;
	uint32		images;
	HistoryRecord record;
} HistoryJournalFrame;

typedef struct HistoryControl
{
	LWLock		lock;
	ConditionVariable changed;
	RelFileLocator locators[HISTORY_MAX_RELATIONS];
	uint32		nrelations;		/* immutable after baseline publication */
	TimeLineID	tli;
	uint32		nrecords;		/* zero until the baseline is published */
	bool		journal_loaded;
	pgoff_t		journal_end;
	bool		stopped;
	char		reason[128];
} HistoryControl;

static int	history_capacity;
static bool history_durable;
static HistoryControl *history;
static HistoryPage *history_pages;
static HistoryRecord *history_records;
static after_wal_replay_hook_type previous_replay_hook;

static void history_request(void *arg);
static void history_initialize(void *arg);
static void history_replay(XLogReaderState *record, TimeLineID tli);
static void history_journal_load(void);
static void history_journal_save(const HistoryRecord *record, uint32 sequence);

static const ForkNumber retained_forks[] = {MAIN_FORKNUM, VISIBILITYMAP_FORKNUM};

static const ShmemCallbacks history_callbacks = {
	.request_fn = history_request,
	.init_fn = history_initialize,
};

void
test_page_store_history_init(void)
{
	DefineCustomIntVariable("test_page_store.history_pages",
							"Maximum retained page images.",
							NULL, &history_capacity, 0, 0, 131072,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("test_page_store.history_durable",
							 "Persist retained history before publishing a record.",
							 NULL, &history_durable, false, PGC_POSTMASTER, 0,
							 NULL, NULL, NULL);
	if (history_durable && (history_capacity == 0 || !enableFsync))
		elog(ERROR, "durable page history requires history_pages > 0 and fsync");
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

/* Every call either transfers the whole item or aborts without publication. */
static void
history_journal_io(int fd, void *data, size_t size, pgoff_t *offset, bool writing)
{
	char	   *p = data;

	while (size > 0)
	{
		size_t		chunk = Min(size, 1024 * 1024);
		ssize_t		transferred;

		errno = 0;
		transferred = writing ? pg_pwrite(fd, p, chunk, *offset) :
			pg_pread(fd, p, chunk, *offset);
		if (transferred < 0 && errno == EINTR)
			continue;
		if (transferred == 0)
			elog(ERROR, "incomplete %s of page history journal at offset " INT64_FORMAT,
				 writing ? "write" : "read", (int64) *offset);
		if (transferred < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not %s page history journal at offset " INT64_FORMAT ": %m",
							writing ? "write" : "read", (int64) *offset)));
		*offset += transferred;
		p += transferred;
		size -= transferred;
		CHECK_FOR_INTERRUPTS();
	}
}

/* Caller holds the history lock; only startup loads after a restart. */
static void
history_journal_load(void)
{
	HistoryJournalHeader header;
	struct stat st;
	pg_crc32c	crc;
	pgoff_t		offset = 0;
	pgoff_t		complete;
	uint32		nrecords = 0;
	uint32		nimages = 0;
	int			fd;

	if (history->journal_loaded)
		return;
	fd = OpenTransientFile(HISTORY_JOURNAL, O_RDWR | PG_BINARY);
	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open page history journal: %m")));
		history->journal_loaded = true;
		return;
	}
	history_journal_io(fd, &header, sizeof(header), &offset, false);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &header, offsetof(HistoryJournalHeader, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, header.crc) ||
		header.magic != HISTORY_JOURNAL_MAGIC ||
		header.version != HISTORY_JOURNAL_VERSION || header.pg_version != PG_VERSION_NUM ||
		header.block_size != BLCKSZ || header.record_size != sizeof(HistoryRecord) ||
		header.image_size != sizeof(HistoryPage) ||
		header.nrelations == 0 || header.nrelations > HISTORY_MAX_RELATIONS)
		elog(ERROR, "invalid page history journal header");
	if (header.system_identifier != GetSystemIdentifier())
		elog(ERROR, "page history journal belongs to another system");
	if (fstat(fd, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat page history journal: %m")));
	complete = offset;
	while (offset < st.st_size)
	{
		HistoryJournalFrame frame;
		pg_crc32c	stored_crc;
		size_t		images_size;

		if (st.st_size - offset < sizeof(frame))
			break;
		history_journal_io(fd, &frame, sizeof(frame), &offset, false);
		if (frame.magic != HISTORY_JOURNAL_MAGIC || frame.sequence != nrecords ||
			frame.images > history_capacity ||
			(uint64) nimages + frame.images != frame.record.nimages ||
			frame.record.nimages > history_capacity || nrecords == HISTORY_MAX_RECORDS ||
			XLogRecPtrIsInvalid(frame.record.lsn) ||
			(nrecords > 0 && frame.record.lsn <= history_records[nrecords - 1].lsn))
			elog(ERROR, "invalid page history journal frame");
		images_size = mul_size(frame.images, sizeof(HistoryPage));
		if (st.st_size - offset < images_size + sizeof(stored_crc))
			break;
		history_journal_io(fd, &history_pages[nimages], images_size, &offset, false);
		history_journal_io(fd, &stored_crc, sizeof(stored_crc), &offset, false);
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, &frame, sizeof(frame));
		COMP_CRC32C(crc, &history_pages[nimages], images_size);
		FIN_CRC32C(crc);
		if (!EQ_CRC32C(crc, stored_crc))
			elog(ERROR, "page history journal checksum mismatch at record %u", nrecords);
		for (uint32 i = nimages; i < frame.record.nimages; i++)
			if (history_pages[i].relation >= header.nrelations ||
				(history_pages[i].forknum != MAIN_FORKNUM &&
				 history_pages[i].forknum != VISIBILITYMAP_FORKNUM) ||
				history_pages[i].block == InvalidBlockNumber)
				elog(ERROR, "invalid page identity in history journal");
		for (uint32 i = 0; i < header.nrelations; i++)
			for (ForkNumber forknum = MAIN_FORKNUM; forknum <= MAX_FORKNUM; forknum++)
			{
				HistoryFile *file = &frame.record.files[i][forknum];

				if (file->floor > frame.record.nimages ||
					(!file->exists && file->nblocks != 0))
					elog(ERROR, "invalid file metadata in history journal");
			}
		history_records[nrecords++] = frame.record;
		nimages = frame.record.nimages;
		complete = offset;
	}
	if (nrecords == 0)
		elog(ERROR, "page history journal has no complete baseline");
	if (complete != st.st_size)
	{
		if (ftruncate(fd, complete) != 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not truncate incomplete page history tail: %m")));
		elog(LOG, "removed incomplete page history journal tail");
	}

	/*
	 * A complete but previously unpublished last frame may not have been
	 * synced.
	 */
	if (pg_fsync(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not sync page history journal: %m")));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close page history journal: %m")));
	memcpy(history->locators, header.locators, sizeof(header.locators));
	history->nrelations = header.nrelations;
	history->tli = header.tli;
	history->journal_end = complete;
	history->nrecords = nrecords;
	history->journal_loaded = true;
}

/* Sync a complete frame before its record boundary is visible to readers. */
static void
history_journal_save(const HistoryRecord *record, uint32 sequence)
{
	HistoryJournalFrame frame = {0};
	uint32		first_image = sequence == 0 ? 0 : history_records[sequence - 1].nimages;
	pgoff_t		offset = history->journal_end;
	pg_crc32c	crc;
	size_t		images_size;
	int			fd;

	/* fsync is reloadable, unlike history_durable. */
	if (!enableFsync)
		elog(ERROR, "durable page history requires fsync");
	fd = OpenTransientFile(sequence == 0 ? HISTORY_JOURNAL_TEMP : HISTORY_JOURNAL,
						   O_RDWR | PG_BINARY | (sequence == 0 ? O_CREAT | O_TRUNC : 0));
	if (fd < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open page history journal: %m")));
	if (sequence == 0)
	{
		HistoryJournalHeader header = {0};

		header.magic = HISTORY_JOURNAL_MAGIC;
		header.version = HISTORY_JOURNAL_VERSION;
		header.pg_version = PG_VERSION_NUM;
		header.block_size = BLCKSZ;
		header.record_size = sizeof(HistoryRecord);
		header.image_size = sizeof(HistoryPage);
		header.system_identifier = GetSystemIdentifier();
		header.nrelations = history->nrelations;
		header.tli = history->tli;
		memcpy(header.locators, history->locators, sizeof(header.locators));
		INIT_CRC32C(header.crc);
		COMP_CRC32C(header.crc, &header, offsetof(HistoryJournalHeader, crc));
		FIN_CRC32C(header.crc);
		offset = 0;
		history_journal_io(fd, &header, sizeof(header), &offset, true);
	}
	frame.magic = HISTORY_JOURNAL_MAGIC;
	frame.sequence = sequence;
	frame.images = record->nimages - first_image;
	frame.record = *record;
	images_size = mul_size(frame.images, sizeof(HistoryPage));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &frame, sizeof(frame));
	COMP_CRC32C(crc, &history_pages[first_image], images_size);
	FIN_CRC32C(crc);
	history_journal_io(fd, &frame, sizeof(frame), &offset, true);
	history_journal_io(fd, &history_pages[first_image], images_size, &offset, true);
	/* The missing checksum makes this an explicitly incomplete tail. */
	if (AmStartupProcess() && frame.images > 0)
		INJECTION_POINT("test-page-store-before-history-footer", NULL);
	history_journal_io(fd, &crc, sizeof(crc), &offset, true);
	if (pg_fsync(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not sync page history journal: %m")));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close page history journal: %m")));
	if (sequence == 0)
		durable_rename(HISTORY_JOURNAL_TEMP, HISTORY_JOURNAL, ERROR);
	else if (frame.images > 0)
		INJECTION_POINT("test-page-store-after-history-sync", NULL);
	history->journal_end = offset;
}

/* The destination must not be in any published record's image range. */
static void
history_copy_page(RelFileLocator locator, uint32 relation, ForkNumber forknum,
				  BlockNumber block, uint32 dest)
{
	Buffer		buffer;
	HistoryPage *page = &history_pages[dest];

	Assert(dest < history_capacity);
	buffer = ReadBufferWithoutRelcache(locator, forknum, block,
									   RBM_NORMAL, NULL, true);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page->relation = relation;
	page->forknum = forknum;
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
		for (int f = 0; f < lengthof(retained_forks); f++)
		{
			ForkNumber	forknum = retained_forks[f];
			HistoryFile *file = &baseline.files[i][forknum];
			SMgrRelation smgr = RelationGetSmgr(rels[i]);

			file->exists = smgrexists(smgr, forknum);
			file->nblocks = file->exists ? smgrnblocks(smgr, forknum) : 0;
			nimages += file->nblocks;
		}
		if (nimages > history_capacity)
			ereport(ERROR, (errmsg("page history capacity is smaller than the baseline")));
	}

	LWLockAcquire(&history->lock, LW_EXCLUSIVE);
	if (history_durable)
		history_journal_load();
	if (history->nrecords != 0)
	{
		LWLockRelease(&history->lock);
		ereport(ERROR, (errmsg("page history is already initialized")));
	}
	for (int i = 0; i < nrelations; i++)
	{
		for (int f = 0; f < lengthof(retained_forks); f++)
		{
			ForkNumber	forknum = retained_forks[f];

			for (BlockNumber block = 0; block < baseline.files[i][forknum].nblocks; block++)
			{
				history_copy_page(rels[i]->rd_locator, i, forknum, block, baseline.nimages++);
				CHECK_FOR_INTERRUPTS();
			}
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
	if (history_durable)
		history_journal_save(&baseline, 0);
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
history_special(XLogReaderState *record, RelFileLocator locator, ForkNumber forknum,
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
					xlrec->forkNum == forknum)
				{
					next->floor = nimages;
					next->exists = true;
					*refresh = true;
				}
			}
			else if (info == XLOG_SMGR_TRUNCATE)
			{
				xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);

				if (RelFileLocatorEquals(locator, xlrec->rlocator))
				{
					if (forknum == MAIN_FORKNUM && (xlrec->flags & SMGR_TRUNCATE_HEAP))
						next->nblocks = xlrec->blkno;
					else if (forknum == VISIBILITYMAP_FORKNUM && (xlrec->flags & SMGR_TRUNCATE_VM))
					{
						/*
						 * Truncation can clear tail bits without a block
						 * reference, even when the VM file doesn't get
						 * shorter.  Copy the remaining map, not just a new
						 * size or referenced pages.
						 */
						*refresh = true;
					}
				}
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
history_replay_file(XLogReaderState *record, uint32 relation,
					ForkNumber forknum, HistoryRecord *next)
{
	RelFileLocator locator = history->locators[relation];
	HistoryFile *file = &next->files[relation][forknum];
	BlockNumber blocks[XLR_MAX_BLOCK_ID + 1];
	BlockNumber copy_from;
	int			nblocks = 0;
	bool		refresh = false;

	if ((XLogRecGetInfo(record) & XLR_SPECIAL_REL_UPDATE) &&
		!history_special(record, locator, forknum, file, next->nimages, &refresh))
	{
		history_stop("unsupported special relation update");
		return false;
	}
	for (int i = 0; i <= XLogRecMaxBlockId(record); i++)
	{
		RelFileLocator rlocator;
		ForkNumber	ref_fork;
		BlockNumber block;

		if (!XLogRecHasBlockRef(record, i))
			continue;
		XLogRecGetBlockTag(record, i, &rlocator, &ref_fork, &block);
		if (ref_fork == forknum && RelFileLocatorEquals(locator, rlocator))
			blocks[nblocks++] = block;
	}
	copy_from = file->nblocks;
	if (refresh || nblocks > 0)
	{
		SMgrRelation smgr = smgropen(locator, INVALID_PROC_NUMBER);

		/* VM is created on demand, without a separate SMGR_CREATE record. */
		if (!file->exists && nblocks > 0 && forknum == VISIBILITYMAP_FORKNUM &&
			next->files[relation][MAIN_FORKNUM].exists)
		{
			file->floor = next->nimages;
			refresh = true;
		}
		if (refresh)
		{
			file->exists = smgrexists(smgr, forknum);
			copy_from = 0;
		}
		if (!file->exists && nblocks > 0)
		{
			history_stop("block reference to an absent retained relation");
			return false;
		}
		file->nblocks = file->exists ? smgrnblocks(smgr, forknum) : 0;
		if (copy_from > file->nblocks)
		{
			history_stop("unexpected fork shrink");
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
		history_copy_page(locator, relation, forknum, block, next->nimages++);
	for (int i = 0; i < nblocks; i++)
	{
		if (blocks[i] >= file->nblocks)
		{
			history_stop("block reference beyond the retained relation");
			return false;
		}
		if (blocks[i] < copy_from)
			history_copy_page(locator, relation, forknum, blocks[i], next->nimages++);
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
	LWLockAcquire(&history->lock, history_durable ? LW_EXCLUSIVE : LW_SHARED);
	if (history_durable)
		history_journal_load();
	if (history->nrecords == 0 || history->stopped)
	{
		LWLockRelease(&history->lock);
		return;
	}
	nrecords = history->nrecords;
	next = history_records[nrecords - 1];
	LWLockRelease(&history->lock);
	/* Restart replays older WAL over md, not over immutable retained images. */
	if (history_durable && record->EndRecPtr <= next.lsn)
		return;
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
	/* Never splice a journal onto a restart point that skipped a history gap. */
	if (history_durable &&
		(record->ReadRecPtr < next.lsn || XLogRecGetPrev(record) >= next.lsn))
	{
		history_stop("replay does not continue the retained journal");
		return;
	}
	Assert(record->EndRecPtr > next.lsn);
	next.lsn = record->EndRecPtr;
	for (uint32 i = 0; i < history->nrelations; i++)
	{
		for (int f = 0; f < lengthof(retained_forks); f++)
		{
			ForkNumber	forknum = retained_forks[f];

			if (!history_replay_file(record, i, forknum, &next))
				return;
			if (history_records[nrecords - 1].files[i][forknum].exists &&
				!next.files[i][forknum].exists)
				INJECTION_POINT("test-page-store-after-history-drop", NULL);
		}
	}

	if (history_durable)
		history_journal_save(&next, nrecords);
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
		(forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM))
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
	file = &record.files[relation][forknum];
	*exists = file->exists;
	*nblocks = file->nblocks;
	if (count > 0 && (!file->exists || (uint64) block + count > file->nblocks))
		ereport(ERROR, (errmsg("requested blocks are outside the retained relation")));
	for (int i = 0; i < count; i++)
	{
		bool		found = false;

		for (uint32 j = record.nimages; j > file->floor; j--)
			if (history_pages[j - 1].relation == relation &&
				history_pages[j - 1].forknum == forknum &&
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
