/* -------------------------------------------------------------------------
 *
 * pgstat_wal.c
 *	  Implementation of WAL statistics.
 *
 * This file contains the implementation of WAL statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_wal.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "executor/instrument.h"
#include "utils/builtins.h"
#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"


PgStat_PendingWalStats PendingWalStats = {0};

/*
 * WAL usage counters saved from pgWalUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by subtracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;


static void lsntime_insert(LSNTimeStream *stream, TimestampTz time, XLogRecPtr lsn);

XLogRecPtr	estimate_lsn_at_time(const LSNTimeStream *stream, TimestampTz time);
TimestampTz estimate_time_at_lsn(const LSNTimeStream *stream, XLogRecPtr lsn);

/*
 * Calculate how much WAL usage counters have increased and update
 * shared WAL and IO statistics.
 *
 * Must be called by processes that generate WAL, that do not call
 * pgstat_report_stat(), like walwriter.
 *
 * "force" set to true ensures that the statistics are flushed; note that
 * this needs to acquire the pgstat shmem LWLock, waiting on it.  When
 * set to false, the statistics may not be flushed if the lock could not
 * be acquired.
 */
void
pgstat_report_wal(bool force)
{
	bool		nowait;

	/* like in pgstat.c, don't wait for lock acquisition when !force */
	nowait = !force;

	/* flush wal stats */
	pgstat_flush_wal(nowait);

	/* flush IO stats */
	pgstat_flush_io(nowait);
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the WAL statistics struct.
 */
PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_WAL);

	return &pgStatLocal.snapshot.wal;
}

/*
 * Calculate how much WAL usage counters have increased by subtracting the
 * previous counters from the current ones.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_flush_wal(bool nowait)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	WalUsage	wal_usage_diff = {0};

	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
	Assert(pgStatLocal.shmem != NULL &&
		   !pgStatLocal.shmem->is_shutdown);

	/*
	 * This function can be called even if nothing at all has happened. Avoid
	 * taking lock for nothing in that case.
	 */
	if (!pgstat_have_pending_wal())
		return false;

	/*
	 * We don't update the WAL usage portion of the local WalStats elsewhere.
	 * Calculate how much WAL usage counters were increased by subtracting the
	 * previous counters from the current ones.
	 */
	WalUsageAccumDiff(&wal_usage_diff, &pgWalUsage, &prevWalUsage);

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;

#define WALSTAT_ACC(fld, var_to_add) \
	(stats_shmem->stats.fld += var_to_add.fld)
#define WALSTAT_ACC_INSTR_TIME(fld) \
	(stats_shmem->stats.fld += INSTR_TIME_GET_MICROSEC(PendingWalStats.fld))
	WALSTAT_ACC(wal_records, wal_usage_diff);
	WALSTAT_ACC(wal_fpi, wal_usage_diff);
	WALSTAT_ACC(wal_bytes, wal_usage_diff);
	WALSTAT_ACC(wal_buffers_full, PendingWalStats);
	WALSTAT_ACC(wal_write, PendingWalStats);
	WALSTAT_ACC(wal_sync, PendingWalStats);
	WALSTAT_ACC_INSTR_TIME(wal_write_time);
	WALSTAT_ACC_INSTR_TIME(wal_sync_time);
#undef WALSTAT_ACC_INSTR_TIME
#undef WALSTAT_ACC

	LWLockRelease(&stats_shmem->lock);

	/*
	 * Save the current counters for the subsequent calculation of WAL usage.
	 */
	prevWalUsage = pgWalUsage;

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingWalStats, 0, sizeof(PendingWalStats));

	return false;
}

void
pgstat_init_wal(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_flush_wal() can
	 * calculate how much pgWalUsage counters are increased by subtracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;
}

/*
 * To determine whether any WAL activity has occurred since last time, not
 * only the number of generated WAL records but also the numbers of WAL
 * writes and syncs need to be checked. Because even transaction that
 * generates no WAL records can write or sync WAL data when flushing the
 * data pages.
 */
bool
pgstat_have_pending_wal(void)
{
	return pgWalUsage.wal_records != prevWalUsage.wal_records ||
		PendingWalStats.wal_write != 0 ||
		PendingWalStats.wal_sync != 0;
}

void
pgstat_wal_reset_all_cb(TimestampTz ts)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	memset(&stats_shmem->stats, 0, sizeof(stats_shmem->stats));
	stats_shmem->stats.stat_reset_timestamp = ts;
	LWLockRelease(&stats_shmem->lock);
}

void
pgstat_wal_snapshot_cb(void)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	memcpy(&pgStatLocal.snapshot.wal, &stats_shmem->stats,
		   sizeof(pgStatLocal.snapshot.wal));
	LWLockRelease(&stats_shmem->lock);
}

/*
 * Given three LSNTimes, calculate the area of the triangle they form were they
 * plotted with time on the X axis and LSN on the Y axis.
 */
static int
lsn_ts_calculate_error_area(LSNTime *left, LSNTime *mid, LSNTime *right)
{
	int			rectangle_all = (right->time - left->time) * (right->lsn - left->lsn);
	int			triangle1 = rectangle_all / 2;
	int			triangle2 = (mid->lsn - left->lsn) * (mid->time - left->time) / 2;
	int			triangle3 = (right->lsn - mid->lsn) * (right->time - mid->time) / 2;
	int			rectangle_part = (right->lsn - mid->lsn) * (mid->time - left->time);

	return rectangle_all - triangle1 - triangle2 - triangle3 - rectangle_part;
}

/*
 * Determine which LSNTime to drop from a full LSNTimeStream. Once the LSNTime
 * is dropped, points between it and either of its adjacent LSNTimes will be
 * interpolated between those two LSNTimes instead. To keep the LSNTimeStream
 * as accurate as possible, drop the LSNTime whose absence would have the least
 * impact on future interpolations.
 *
 * We determine the error that would be introduced by dropping a point on the
 * stream by calculating the area of the triangle formed by the LSNTime and its
 * adjacent LSNTimes. We do this for each LSNTime in the stream (except for the
 * first and last LSNTimes) and choose the LSNTime with the smallest error
 * (area). We avoid extrapolation by never dropping the first or last points.
 */
static int
lsntime_to_drop(LSNTimeStream *stream)
{
	int			min_area = INT_MAX;
	int			target_point = stream->length - 1;

	/* Don't drop points if free space available */
	Assert(stream->length == LSNTIMESTREAM_VOLUME);

	for (int i = stream->length - 1; i-- > 0;)
	{
		LSNTime    *left = &stream->data[i - 1];
		LSNTime    *mid = &stream->data[i];
		LSNTime    *right = &stream->data[i + 1];
		int			area = lsn_ts_calculate_error_area(left, mid, right);

		if (abs(area) < abs(min_area))
		{
			min_area = area;
			target_point = i;
		}
	}

	return target_point;
}

/*
 * Insert a new LSNTime into the LSNTimeStream in the first available element,
 * or, if there are no empty elements, drop an LSNTime from the stream, move
 * all LSNTimes down and insert the new LSNTime into the element at index 0.
 */
void
lsntime_insert(LSNTimeStream *stream, TimestampTz time,
			   XLogRecPtr lsn)
{
	int			drop;
	LSNTime		entrant = {.lsn = lsn,.time = time};

	if (stream->length < LSNTIMESTREAM_VOLUME)
	{
		/*
		 * The new entry should exceed the most recent entry to ensure time
		 * moves forward on the stream.
		 */
		Assert(stream->length == 0 ||
			   (lsn >= stream->data[LSNTIMESTREAM_VOLUME - stream->length].lsn &&
				time >= stream->data[LSNTIMESTREAM_VOLUME - stream->length].time));

		/*
		 * If there are unfilled elements in the stream, insert the passed-in
		 * LSNTime into the tail of the array.
		 */
		stream->length++;
		stream->data[LSNTIMESTREAM_VOLUME - stream->length] = entrant;
		return;
	}

	drop = lsntime_to_drop(stream);
	if (drop < 0 || drop >= stream->length)
	{
		elog(WARNING, "Unable to insert LSNTime to LSNTimeStream. Drop failed.");
		return;
	}

	/*
	 * Drop the LSNTime at index drop by copying the array from drop - 1 to
	 * drop
	 */
	memmove(&stream->data[1], &stream->data[0], sizeof(LSNTime) * drop);
	stream->data[0] = entrant;
}

/*
 * Translate time to a LSN using the provided stream. The stream will not
 * be modified.
 */
XLogRecPtr
estimate_lsn_at_time(const LSNTimeStream *stream, TimestampTz time)
{
	XLogRecPtr	result;
	int64		time_elapsed,
				lsns_elapsed;
	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the provided time is before DB startup, the best we can do is return
	 * the start LSN.
	 */
	if (time < start.time)
		return start.lsn;

	/*
	 * If the provided time is after now, the current LSN is our best
	 * estimate.
	 */
	if (time >= end.time)
		return end.lsn;

	/*
	 * Loop through the stream. Stop at the first LSNTime earlier than our
	 * target time. This LSNTime will be our interpolation start point. If
	 * there's an LSNTime later than that, then that will be our interpolation
	 * end point.
	 */
	for (int i = LSNTIMESTREAM_VOLUME - stream->length; i < LSNTIMESTREAM_VOLUME; i++)
	{
		if (stream->data[i].time > time)
			continue;

		start = stream->data[i];
		if (i > LSNTIMESTREAM_VOLUME)
			end = stream->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the stream, then use its earliest LSNTime as our
	 * interpolation end point.
	 */
	if (stream->length > 0)
		end = stream->data[LSNTIMESTREAM_VOLUME - 1];

stop:
	Assert(end.time > start.time);
	Assert(end.lsn > start.lsn);
	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);
	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);
	result = (double) (time - start.time) / time_elapsed * lsns_elapsed + start.lsn;
	return Max(result, 0);
}

/*
 * Translate lsn to a time using the provided stream. The stream will not
 * be modified.
 */
TimestampTz
estimate_time_at_lsn(const LSNTimeStream *stream, XLogRecPtr lsn)
{
	int64		time_elapsed,
				lsns_elapsed;
	TimestampTz result;
	LSNTime		start = {.time = PgStartTime,.lsn = PgStartLSN};
	LSNTime		end = {.time = GetCurrentTimestamp(),.lsn = GetXLogInsertRecPtr()};

	/*
	 * If the LSN is before DB startup, the best we can do is return that
	 * time.
	 */
	if (lsn <= start.lsn)
		return start.time;

	/*
	 * If the target LSN is after the current insert LSN, the current time is
	 * our best estimate.
	 */
	if (lsn >= end.lsn)
		return end.time;

	/*
	 * Loop through the stream. Stop at the first LSNTime earlier than our
	 * target LSN. This LSNTime will be our interpolation start point. If
	 * there's an LSNTime later than that, then that will be our interpolation
	 * end point.
	 */
	for (int i = LSNTIMESTREAM_VOLUME - stream->length; i < LSNTIMESTREAM_VOLUME; i++)
	{
		if (stream->data[i].lsn > lsn)
			continue;

		start = stream->data[i];
		if (i > LSNTIMESTREAM_VOLUME - stream->length)
			end = stream->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the stream, then use its earliest LSNTime as our
	 * interpolation end point.
	 */
	if (stream->length > 0)
		end = stream->data[LSNTIMESTREAM_VOLUME - 1];

stop:
	Assert(end.time > start.time);
	Assert(end.lsn > start.lsn);
	time_elapsed = end.time - start.time;
	Assert(time_elapsed != 0);
	lsns_elapsed = end.lsn - start.lsn;
	Assert(lsns_elapsed != 0);
	result = (double) (lsn - start.lsn) / lsns_elapsed * time_elapsed + start.time;
	return Max(result, 0);
}
