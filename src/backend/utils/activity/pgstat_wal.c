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
#include "math.h"
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
pgstat_wal_init_shmem_cb(void *stats)
{
	PgStatShared_Wal *stats_shmem = (PgStatShared_Wal *) stats;

	LWLockInitialize(&stats_shmem->lock, LWTRANCHE_PGSTATS_DATA);
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
 * plotted with time on the X axis and LSN on the Y axis. An illustration:
 *
 *   LSN
 *    |
 *    |                                                         * right
 *    |
 *    |
 *    |
 *    |                                                * mid    * C
 *    |
 *    |
 *    |
 *    |  * left                                        * B      * A
 *    |
 *    +------------------------------------------------------------------
 *
 * The area of the triangle with vertices (left, mid, right) is the error
 * incurred over the interval [left, right] were we to interpolate with just
 * [left, right] rather than [left, mid) and [mid, right).
 */
static float
lsn_ts_calculate_error_area(LSNTime *left, LSNTime *mid, LSNTime *right)
{
	float		left_time = left->time,
				left_lsn = left->lsn;
	float		mid_time = mid->time,
				mid_lsn = mid->lsn;
	float		right_time = right->time,
				right_lsn = right->lsn;

	/* Area of the rectangle with opposing corners left and right */
	float		rectangle_all = (right_time - left_time) * (right_lsn - left_lsn);

	/* Area of the right triangle with vertices left, right, and A */
	float		triangle1 = rectangle_all / 2;

	/* Area of the right triangle with vertices left, mid, and B */
	float		triangle2 = (mid_lsn - left_lsn) * (mid_time - left_time) / 2;

	/* Area of the right triangle with vertices mid, right, and C */
	float		triangle3 = (right_lsn - mid_lsn) * (right_time - mid_time) / 2;

	/* Area of the rectangle with vertices mid, A, B, and C */
	float		rectangle_part = (right_lsn - mid_lsn) * (mid_time - left_time);

	/* Area of the triangle with vertices left, mid, and right */
	return triangle1 - triangle2 - triangle3 - rectangle_part;
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
static unsigned int
lsntime_to_drop(LSNTimeStream *stream)
{
	double		min_area;
	unsigned int target_point;

	/* Don't drop points if free space available */
	Assert(stream->length == LSNTIMESTREAM_VOLUME);

	min_area = lsn_ts_calculate_error_area(&stream->data[0],
										   &stream->data[1],
										   &stream->data[2]);

	target_point = 1;

	for (int i = 1; i < stream->length - 1; i++)
	{
		LSNTime    *left = &stream->data[i - 1];
		LSNTime    *mid = &stream->data[i];
		LSNTime    *right = &stream->data[i + 1];
		float		area = lsn_ts_calculate_error_area(left, mid, right);

		if (fabs(area) < fabs(min_area))
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
 * all the subsequent LSNTimes down and insert the new LSNTime into the tail.
 */
void
lsntime_insert(LSNTimeStream *stream, TimestampTz time,
			   XLogRecPtr lsn)
{
	unsigned int drop;
	LSNTime		entrant = {.lsn = lsn,.time = time};

	if (stream->length < LSNTIMESTREAM_VOLUME)
	{
		/*
		 * The new entry should exceed the most recent entry to ensure time
		 * moves forward on the stream.
		 */
		Assert(stream->length == 0 ||
			   (lsn >= stream->data[stream->length - 1].lsn &&
				time >= stream->data[stream->length - 1].time));

		/*
		 * If there are unfilled elements in the stream, insert the passed-in
		 * LSNTime into the current tail of the array.
		 */
		stream->data[stream->length++] = entrant;
		return;
	}

	drop = lsntime_to_drop(stream);

	/*
	 * Drop the LSNTime at index drop by copying the array from drop - 1 to
	 * drop
	 */
	memmove(&stream->data[drop],
			&stream->data[drop + 1],
			sizeof(LSNTime) * (stream->length - 1 - drop));

	stream->data[stream->length - 1] = entrant;
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
	 * If the database has been restarted, PgStartLSN may be after our oldest
	 * value. In that case, use the oldest value in the time stream as the
	 * start.
	 */
	if (stream->length > 0 && start.time > stream->data[0].time)
		start = stream->data[0];

	/*
	 * If the LSN is before our oldest known LSN, the best we can do is return
	 * our oldest known time.
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
	 * Loop through the stream. Stop at the first LSNTime later than our
	 * target time. This LSNTime will be our interpolation end point. If
	 * there's an LSNTime earlier than that, that will be our interpolation
	 * start point.
	 */
	for (int i = 0; i < stream->length; i++)
	{
		if (stream->data[i].time < time)
			continue;

		end = stream->data[i];
		if (i > 0)
			start = stream->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the stream, then use its latest LSNTime as our
	 * interpolation start point.
	 */
	if (stream->length > 0)
		start = stream->data[stream->length - 1];

stop:

	/*
	 * In rare cases, the start and end LSN could be the same. If, for
	 * example, no new records have been inserted since the last one recorded
	 * in the LSNTimeStream and we are looking for the LSN corresponding to
	 * the current time.
	 */
	if (end.lsn == start.lsn)
		return end.lsn;

	Assert(end.lsn > start.lsn);

	/*
	 * It should be extremely rare (if not impossible) for the start time and
	 * end time to be the same. In this case, just return an LSN halfway
	 * between the two.
	 */
	if (end.time == start.time)
		return start.lsn + ((end.lsn - start.lsn) / 2);

	Assert(end.time > start.time);

	time_elapsed = end.time - start.time;
	lsns_elapsed = end.lsn - start.lsn;

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
	 * If the database has been restarted, PgStartLSN may be after our oldest
	 * value. In that case, use the oldest value in the time stream as the
	 * start.
	 */
	if (stream->length > 0 && start.time > stream->data[0].time)
		start = stream->data[0];

	/*
	 * If the LSN is before our oldest known LSN, the best we can do is return
	 * our oldest known time.
	 */
	if (lsn < start.lsn)
		return start.time;

	/*
	 * If the target LSN is after the current insert LSN, the current time is
	 * our best estimate.
	 */
	if (lsn >= end.lsn)
		return end.time;

	/*
	 * Loop through the stream. Stop at the first LSNTime later than our
	 * target time. This LSNTime will be our interpolation end point. If
	 * there's an LSNTime earlier than that, that will be our interpolation
	 * start point.
	 */
	for (int i = 0; i < stream->length; i++)
	{
		if (stream->data[i].lsn < lsn)
			continue;

		end = stream->data[i];
		if (i > 0)
			start = stream->data[i - 1];
		goto stop;
	}

	/*
	 * If we exhausted the stream, then use its earliest LSNTime as our
	 * interpolation end point.
	 */
	if (stream->length > 0)
		start = stream->data[stream->length - 1];

stop:

	/* It should be nearly impossible to have the same start and end time. */
	if (end.time == start.time)
		return end.time;

	Assert(end.time > start.time);

	/*
	 * In rare cases, the start and end LSN could be the same. If, for
	 * example, no new records have been inserted since the last one recorded
	 * in the LSNTimeStream and we are looking for the LSN corresponding to
	 * the current time. In this case, just return a time halfway between
	 * start and end.
	 */
	if (end.lsn == start.lsn)
		return start.time + ((end.time - start.time) / 2);

	Assert(end.lsn > start.lsn);

	time_elapsed = end.time - start.time;
	lsns_elapsed = end.lsn - start.lsn;

	result = (double) (lsn - start.lsn) / lsns_elapsed * time_elapsed + start.time;
	return Max(result, 0);
}

void
pgstat_wal_update_lsntime_stream(TimestampTz time, XLogRecPtr lsn)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	lsntime_insert(&stats_shmem->stats.stream, time, lsn);
	LWLockRelease(&stats_shmem->lock);
}
