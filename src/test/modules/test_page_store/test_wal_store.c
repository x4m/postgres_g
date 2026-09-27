/*-------------------------------------------------------------------------
 *
 * test_wal_store.c
 *      A single-stream durable WAL inbox, independent of compute startup.
 *
 * This is deliberately not the service postmaster's own pg_wal.  A private
 * control file names the compute's system identifier, timeline and writer
 * epoch, and the contiguous durable range in an append-only byte file.
 * Ordinary PostgreSQL redo will interpret these bytes on a separate node.
 *
 * There is no election, timeline branching, retention/GC, or quorum here.
 * A trusted controller supplies epochs; a higher epoch only fences future
 * appends to this store, not SQL execution or delivery of earlier replies.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "common/file_perm.h"
#include "libpq/pqformat.h"
#include "libpq/protocol.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/injection_point.h"

#include "test_page_store.h"

#define WAL_STORE_DIR "test_page_store.wal"
#define WAL_STORE_DATA WAL_STORE_DIR "/bytes"
#define WAL_STORE_CONTROL WAL_STORE_DIR "/control"
#define WAL_STORE_TEMP WAL_STORE_DIR "/control.tmp"
#define WAL_STORE_MAGIC 0x54505753
#define WAL_STORE_VERSION 1

typedef struct WalStoreControl
{
	uint32		magic;
	uint32		version;
	uint64		system_identifier;
	TimeLineID	tli;
	uint32		segment_size;
	uint64		epoch;
	XLogRecPtr	start;
	XLogRecPtr	flushed;
	pg_crc32c	crc;
} WalStoreControl;

static bool wal_store_enabled;
static int	wal_store_max_mb;
static LWLock *wal_store_lock;

static void wal_store_request_shmem(void *arg);
static void wal_store_initialize(void *arg);
static bool wal_store_load(WalStoreControl *control);

static const ShmemCallbacks wal_store_callbacks = {
	.request_fn = wal_store_request_shmem,
	.init_fn = wal_store_initialize,
};

void
test_page_store_wal_store_init(void)
{
	DefineCustomBoolVariable("test_page_store.wal_store",
							 "Accept a separate compute's WAL through physical sessions.",
							 NULL, &wal_store_enabled, false, PGC_POSTMASTER, 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.wal_store_max_mb",
							"Maximum retained WAL in the test inbox.",
							NULL, &wal_store_max_mb, 1024, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MB, NULL, NULL, NULL);
	if (wal_store_enabled && !enableFsync)
		elog(ERROR, "test WAL store requires fsync");
	RegisterShmemCallbacks(&wal_store_callbacks);
}

bool
test_page_store_wal_store_enabled(void)
{
	return wal_store_enabled;
}

static void
wal_store_request_shmem(void *arg)
{
	if (wal_store_enabled)
		ShmemRequestStruct(.name = "test_page_store WAL inbox",
						   .size = sizeof(LWLock), .ptr = (void **) &wal_store_lock);
}

static void
wal_store_io(int fd, void *data, size_t size, pgoff_t offset, bool writing)
{
	char	   *p = data;

	while (size > 0)
	{
		ssize_t		n = writing ? pg_pwrite(fd, p, size, offset) :
			pg_pread(fd, p, size, offset);

		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not %s test WAL store: %m", writing ? "write" : "read")));
		if (n == 0)
			elog(ERROR, "incomplete %s of test WAL store", writing ? "write" : "read");
		p += n;
		size -= n;
		offset += n;
	}
}

static int
wal_store_open(const char *path, int flags)
{
	int			fd = OpenTransientFile(path, flags | PG_BINARY);

	if (fd < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open test WAL store file \"%s\": %m", path)));
	return fd;
}

static void
wal_store_close(int fd)
{
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not close test WAL store file: %m")));
}

/* No shared copy can get ahead of the atomically replaced control file. */
static bool
wal_store_load(WalStoreControl *control)
{
	int			fd;
	struct stat st;
	pg_crc32c	crc;

	fd = OpenTransientFile(WAL_STORE_CONTROL, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
		{
			/* Never mistake lost authority metadata for a fresh namespace. */
			if (stat(WAL_STORE_DATA, &st) == 0)
				elog(ERROR, "test WAL store control is missing for existing bytes");
			if (errno == ENOENT)
				return false;
		}
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open test WAL store control: %m")));
	}
	if (fstat(fd, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat test WAL store control: %m")));
	if (st.st_size != sizeof(*control))
		elog(ERROR, "invalid test WAL store control size");
	wal_store_io(fd, control, sizeof(*control), 0, false);
	wal_store_close(fd);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, control, offsetof(WalStoreControl, crc));
	FIN_CRC32C(crc);
	if (control->magic != WAL_STORE_MAGIC || control->version != WAL_STORE_VERSION ||
		!EQ_CRC32C(crc, control->crc) || control->system_identifier == 0 ||
		control->tli == 0 || control->epoch == 0 ||
		!IsValidWalSegSize(control->segment_size) ||
		XLogRecPtrIsInvalid(control->start) ||
		control->start % control->segment_size != 0 ||
		control->flushed < control->start ||
		control->flushed - control->start > PG_INT64_MAX)
		elog(ERROR, "invalid test WAL store control");
	if (stat(WAL_STORE_DATA, &st) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat test WAL store bytes: %m")));
	if (st.st_size < control->flushed - control->start)
		elog(ERROR, "test WAL store is shorter than its durable frontier");
	return true;
}

static void
wal_store_initialize(void *arg)
{
	WalStoreControl control;

	if (!wal_store_enabled)
		return;
	LWLockInitialize(wal_store_lock, LWLockNewTrancheId("test_page_store WAL inbox"));
	if (mkdir(WAL_STORE_DIR, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(FATAL, (errcode_for_file_access(), errmsg("could not create test WAL store directory: %m")));
	fsync_fname_ext(".", true, false, PANIC);
	if (!wal_store_load(&control))
		return;

	/* Only the published prefix survives; an incomplete append has no force. */
	if (pg_truncate(WAL_STORE_DATA, control.flushed - control.start) != 0)
		ereport(FATAL, (errcode_for_file_access(), errmsg("could not truncate test WAL store bytes: %m")));
	fsync_fname_ext(WAL_STORE_DATA, false, false, PANIC);
	fsync_fname_ext(WAL_STORE_CONTROL, false, false, PANIC);
	fsync_fname_ext(WAL_STORE_DIR, true, false, PANIC);
}

static void
wal_store_save(WalStoreControl *control)
{
	int			fd;

	INIT_CRC32C(control->crc);
	COMP_CRC32C(control->crc, control, offsetof(WalStoreControl, crc));
	FIN_CRC32C(control->crc);
	fd = wal_store_open(WAL_STORE_TEMP, O_WRONLY | O_CREAT | O_TRUNC);
	wal_store_io(fd, control, sizeof(*control), 0, true);
	wal_store_close(fd);

	/* Do not keep serving after an uncertain publication or directory fsync. */
	START_CRIT_SECTION();
	durable_rename(WAL_STORE_TEMP, WAL_STORE_CONTROL, PANIC);
	END_CRIT_SECTION();
	INJECTION_POINT("test-wal-store-after-publish", NULL);
}

/*
 * One lock orders initialization, appends and fencing, including their fsyncs.
 * No network I/O or response publication occurs while holding it.  This is a
 * correctness scaffold, not a group-commit or throughput implementation.
 */
void
test_page_store_wal_store_request(StringInfo request, StringInfo response)
{
	int			kind = pq_getmsgbyte(request);
	uint64		sysid = pq_getmsgint64(request);
	TimeLineID	tli = pq_getmsgint(request, 4);
	uint64		epoch = pq_getmsgint64(request);
	XLogRecPtr	lsn = pq_getmsgint64(request);
	uint32		size = 0;
	const char *data = NULL;
	char	   *bytes = NULL;
	WalStoreControl control;
	bool		found;

	if (!wal_store_enabled || !enableFsync)
		elog(ERROR, "test WAL store requires wal_store and fsync");
	if (sysid == 0 || tli == 0)
		elog(ERROR, "invalid test WAL stream identity");
	if (kind == 'i' || kind == 'r')
		size = pq_getmsgint(request, 4);
	else if (kind == 'a')
	{
		size = request->len - request->cursor;
		data = pq_getmsgbytes(request, size);
	}
	else if (kind != 's' && kind != 'f')
		elog(ERROR, "unknown test WAL request type: %d", kind);
	pq_getmsgend(request);
	if (kind == 'i' && (!IsValidWalSegSize(size) || epoch == 0 ||
						XLogRecPtrIsInvalid(lsn) || lsn % size != 0))
		elog(ERROR, "invalid test WAL store initialization");
	if ((kind == 'a' || kind == 'r') &&
		(size == 0 || size > TEST_WAL_STORE_MAX_BYTES || lsn > PG_UINT64_MAX - size))
		elog(ERROR, "invalid test WAL byte range");
	if ((kind == 's' || kind == 'f') && !XLogRecPtrIsInvalid(lsn))
		elog(ERROR, "unexpected LSN in test WAL control request");

	LWLockAcquire(wal_store_lock, LW_EXCLUSIVE);
	found = wal_store_load(&control);
	if (!found)
	{
		int			fd;

		if (kind != 'i')
			elog(ERROR, "test WAL store is not initialized");
		memset(&control, 0, sizeof(control));
		control.magic = WAL_STORE_MAGIC;
		control.version = WAL_STORE_VERSION;
		control.system_identifier = sysid;
		control.tli = tli;
		control.epoch = epoch;
		control.segment_size = size;
		control.start = control.flushed = lsn;
		fd = wal_store_open(WAL_STORE_DATA, O_WRONLY | O_CREAT | O_TRUNC);
		wal_store_close(fd);
		fsync_fname_ext(WAL_STORE_DATA, false, false, PANIC);
		fsync_fname_ext(WAL_STORE_DIR, true, false, PANIC);
		wal_store_save(&control);
	}
	if (sysid != control.system_identifier || tli != control.tli)
		elog(ERROR, "test WAL stream identity does not match");
	if (kind != 's' && epoch != control.epoch)
		elog(ERROR, "test WAL writer epoch does not match");
	if (kind == 'i' && (lsn != control.start || size != control.segment_size))
		elog(ERROR, "test WAL store initialization does not match");
	if (kind == 'f')
	{
		if (control.epoch == PG_UINT64_MAX)
			elog(ERROR, "test WAL writer epoch is exhausted");
		control.epoch++;
		wal_store_save(&control);
	}
	else if (kind == 'a' || kind == 'r')
	{
		int			fd;
		XLogRecPtr	end = lsn + size;

		if (lsn < control.start || lsn > control.flushed)
			elog(ERROR, "test WAL range is not contiguous with retained bytes");
		if (kind == 'r' && end > control.flushed)
			elog(ERROR, "test WAL read exceeds durable frontier");
		if (kind == 'a' && end - control.start > (uint64) wal_store_max_mb * 1024 * 1024)
			elog(ERROR, "test WAL store capacity exceeded");
		fd = wal_store_open(WAL_STORE_DATA, kind == 'r' ? O_RDONLY : O_RDWR);
		bytes = palloc(size);
		if (kind == 'r')
			wal_store_io(fd, bytes, size, lsn - control.start, false);
		else
		{
			size_t		overlap = Min(end, control.flushed) - lsn;

			/*
			 * A retry may overlap, but can never replace acknowledged WAL.
			 */
			wal_store_io(fd, bytes, overlap, lsn - control.start, false);
			if (memcmp(bytes, data, overlap) != 0)
				elog(ERROR, "test WAL retry disagrees with durable bytes");
			if (end > control.flushed)
			{
				wal_store_io(fd, unconstify(char *, data + overlap), size - overlap,
							 control.flushed - control.start, true);
				INJECTION_POINT("test-wal-store-before-data-sync", NULL);
				if (pg_fsync(fd) != 0)
					ereport(PANIC, (errcode_for_file_access(), errmsg("could not sync test WAL store bytes: %m")));
				INJECTION_POINT("test-wal-store-after-data-sync", NULL);
				control.flushed = end;
				wal_store_save(&control);
			}
		}
		wal_store_close(fd);
	}
	LWLockRelease(wal_store_lock);

	pq_beginmessage(response, PqMsg_CopyData);
	pq_sendbyte(response, kind);
	pq_sendint64(response, control.system_identifier);
	pq_sendint32(response, control.tli);
	pq_sendint64(response, control.epoch);
	pq_sendint64(response, lsn);
	pq_sendint64(response, control.start);
	pq_sendint64(response, control.flushed);
	pq_sendint32(response, control.segment_size);
	if (kind == 'r')
		pq_sendbytes(response, bytes, size);
	pq_endmessage(response);
}
