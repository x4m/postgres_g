/*-------------------------------------------------------------------------
 *
 * test_wal_fetch.c
 *      Materialize a bounded WAL inbox prefix for ordinary recovery.
 *
 * This is a controller-side prototype, not a general restore_command.
 * The destination must not exist.  Only a successful exit and the final
 * manifest publish the bundle; a failed run leaves diagnostic partial files.
 * Zero padding of the last segment is not part of the acknowledged prefix.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "libpq-fe.h"
#include "port/pg_bswap.h"
#include "portability/instr_time.h"

#include "test_page_wire.h"

static PGconn *conn;
static uint64 system_identifier;
static uint32 timeline;
static uint64 epoch;
static uint64 start_lsn;
static uint64 end_lsn;
static uint32 segment_size;
static instr_time request_start;

static uint64
parse_number(const char *value)
{
	char	   *end;
	uint64		result;

	errno = 0;
	result = strtou64(value, &end, 10);
	if (errno || value[0] < '0' || value[0] > '9' || *end || !result)
		pg_fatal("invalid positive integer: %s", value);
	return result;
}

static uint32
read32(const char *data)
{
	uint32		result;

	memcpy(&result, data, sizeof(result));
	return pg_ntoh32(result);
}

static uint64
read64(const char *data)
{
	uint64		result;

	memcpy(&result, data, sizeof(result));
	return pg_ntoh64(result);
}

static void
write32(char *data, uint32 value)
{
	value = pg_hton32(value);
	memcpy(data, &value, sizeof(value));
}

static void
write64(char *data, uint64 value)
{
	value = pg_hton64(value);
	memcpy(data, &value, sizeof(value));
}

/* Bound each exchange, not merely connection establishment. */
static void
wait_socket(bool writing, bool connecting)
{
	int			socket = PQsocket(conn);
	fd_set		reads;
	fd_set		writes;
	struct timeval timeout;
	instr_time	elapsed;
	long		remaining;
	int			ret;

	INSTR_TIME_SET_CURRENT(elapsed);
	INSTR_TIME_SUBTRACT(elapsed, request_start);
	remaining = 30000 - (long) INSTR_TIME_GET_MILLISEC(elapsed);
	if (remaining <= 0)
		pg_fatal("WAL inbox request timed out");
	if (socket < 0)
		pg_fatal("invalid WAL inbox socket: %s", PQerrorMessage(conn));
#ifndef WIN32
	if (socket >= FD_SETSIZE)
		pg_fatal("WAL inbox socket exceeds select limit");
#endif
	FD_ZERO(&reads);
	FD_ZERO(&writes);
	FD_SET(socket, &reads);
	if (writing)
		FD_SET(socket, &writes);
	timeout.tv_sec = remaining / 1000;
	timeout.tv_usec = (remaining % 1000) * 1000;
	ret = select(socket + 1, &reads, writing ? &writes : NULL, NULL, &timeout);
	if (ret < 0 && errno != EINTR)
		pg_fatal("select failed: %m");
	if (ret > 0 && !connecting && FD_ISSET(socket, &reads) && !PQconsumeInput(conn))
		pg_fatal("could not read WAL inbox response: %s", PQerrorMessage(conn));
}

static void
flush_request(void)
{
	int			ret;

	while ((ret = PQflush(conn)) > 0)
		wait_socket(true, false);
	if (ret < 0)
		pg_fatal("could not flush WAL inbox request: %s", PQerrorMessage(conn));
}

static PGresult *
get_result(void)
{
	while (PQisBusy(conn))
		wait_socket(false, false);
	return PQgetResult(conn);
}

static char *
get_response(int expected_size)
{
	char	   *data = NULL;
	int			size;

	while ((size = PQgetCopyData(conn, &data, 1)) == 0)
		wait_socket(false, false);
	if (size < 0)
	{
		PGresult   *result = get_result();

		pg_fatal("WAL inbox session ended: %s",
				 result ? PQresultErrorMessage(result) : PQerrorMessage(conn));
	}
	if (size != expected_size)
		pg_fatal("unexpected WAL inbox response length: %d", size);
	return data;
}

static void
connect_inbox(const char *conninfo)
{
	const char *keys[] = {"dbname", "replication", "application_name", NULL};
	const char *values[] = {conninfo, "true", "test WAL fetch", NULL};
	PGresult   *result;
	char	   *greeting;

	INSTR_TIME_SET_CURRENT(request_start);
	conn = PQconnectStartParams(keys, values, 1);
	if (!conn)
		pg_fatal("could not allocate WAL inbox connection");
	for (;;)
	{
		PostgresPollingStatusType status = PQconnectPoll(conn);

		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED)
			pg_fatal("could not connect to WAL inbox: %s", PQerrorMessage(conn));
		wait_socket(status != PGRES_POLLING_READING, true);
	}
	if (PQsetnonblocking(conn, 1) != 0 || !PQsendQuery(conn, TEST_WAL_SERVICE_COMMAND))
		pg_fatal("could not start WAL inbox session: %s", PQerrorMessage(conn));
	flush_request();
	result = get_result();
	if (!result || PQresultStatus(result) != PGRES_COPY_BOTH)
		pg_fatal("could not start WAL inbox service: %s",
				 result ? PQresultErrorMessage(result) : PQerrorMessage(conn));
	PQclear(result);
	greeting = get_response(21);
	if (greeting[0] != 'h' || read32(greeting + 1) != TEST_PAGE_SERVICE_VERSION ||
		read32(greeting + 5) != PG_VERSION_NUM || read32(greeting + 9) != BLCKSZ ||
		read64(greeting + 13) == 0)
		pg_fatal("incompatible WAL inbox greeting");
	PQfreemem(greeting);
}

static char *
exchange(char kind, uint64 lsn, uint32 count)
{
	char		request[33];
	char	   *response;
	int			sent;

	INSTR_TIME_SET_CURRENT(request_start);
	request[0] = kind;
	write64(request + 1, system_identifier);
	write32(request + 9, timeline);
	write64(request + 13, epoch);
	write64(request + 21, lsn);
	write32(request + 29, count);
	while ((sent = PQputCopyData(conn, request, kind == 'r' ? 33 : 29)) == 0)
		flush_request();
	if (sent < 0)
		pg_fatal("could not send WAL inbox request: %s", PQerrorMessage(conn));
	flush_request();
	response = get_response(49 + count);
	if (response[0] != kind || read64(response + 1) != system_identifier ||
		read32(response + 9) != timeline || read64(response + 21) != lsn ||
		read64(response + 13) != epoch + (kind == 'f' ? 1 : 0))
		pg_fatal("WAL inbox response identity or epoch does not match");
	return response;
}

static void
write_all(int fd, const char *data, size_t size)
{
	while (size > 0)
	{
		ssize_t		written = write(fd, data, size);

		if (written < 0 && errno == EINTR)
			continue;
		if (written <= 0)
			pg_fatal("could not write WAL bundle: %m");
		data += written;
		size -= written;
	}
}

int
main(int argc, char **argv)
{
	bool		fence = argc > 1 && strcmp(argv[1], "--fence") == 0;
	int			base = fence ? 2 : 1;
	const char *directory;
	char	   *response;
	char		path[MAXPGPATH];
	char		temporary[MAXPGPATH];
	char		manifest[512];
	uint64		parsed_tli;
	int			fd;
	int			length;

	pg_logging_init(argv[0]);
	pg_initialize_timing();
	if (argc != base + 5)
		pg_fatal("usage: %s [--fence] CONNINFO SYSTEM_ID TIMELINE EPOCH NEW_DIRECTORY", argv[0]);
	system_identifier = parse_number(argv[base + 1]);
	parsed_tli = parse_number(argv[base + 2]);
	if (parsed_tli > PG_UINT32_MAX)
		pg_fatal("timeline is out of range");
	timeline = parsed_tli;
	epoch = parse_number(argv[base + 3]);
	if (fence && epoch == PG_UINT64_MAX)
		pg_fatal("writer epoch is exhausted");
	directory = argv[base + 4];
	if (strlen(directory) + 32 >= MAXPGPATH)
		pg_fatal("WAL bundle path is too long");
	/* Refuse reuse before a potentially irreversible fencing operation. */
	if (mkdir(directory, 0700) != 0)
		pg_fatal("could not create new WAL bundle directory: %m");
	connect_inbox(argv[base]);
	response = exchange(fence ? 'f' : 's', 0, 0);
	if (fence)
		epoch++;
	start_lsn = read64(response + 29);
	end_lsn = read64(response + 37);
	segment_size = read32(response + 45);
	PQfreemem(response);
	if (!IsValidWalSegSize(segment_size) || !start_lsn ||
		start_lsn % segment_size != 0 || end_lsn < start_lsn)
		pg_fatal("invalid WAL inbox range");

	for (uint64 lsn = start_lsn; lsn < end_lsn;)
	{
		char		filename[MAXFNAMELEN];
		uint64		end = lsn + Min(end_lsn - lsn, segment_size);

		XLogFileName(filename, timeline, lsn / segment_size, segment_size);
		snprintf(path, sizeof(path), "%s/%s", directory, filename);
		fd = open(path, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY, 0600);
		if (fd < 0)
			pg_fatal("could not create WAL segment: %m");
		while (lsn < end)
		{
			uint32		count = Min(end - lsn, TEST_WAL_STORE_MAX_BYTES);

			response = exchange('r', lsn, count);
			if (read64(response + 29) != start_lsn || read64(response + 37) < end_lsn ||
				read32(response + 45) != segment_size)
				pg_fatal("WAL inbox prefix changed during export");
			write_all(fd, response + 49, count);
			PQfreemem(response);
			lsn += count;
		}

		/*
		 * A new file has no old tail to expose.  Padding is never claimed
		 * durable WAL.
		 */
		if (ftruncate(fd, segment_size) != 0 || close(fd) != 0 || fsync_fname(path, false) != 0)
			pg_fatal("could not finish WAL segment: %m");
	}
	PQfinish(conn);

	/* Publish only after every segment and its directory entry are durable. */
	if (fsync_fname(directory, true) != 0 || fsync_parent_path(directory) != 0)
		pg_fatal("could not sync WAL bundle directory: %m");
	snprintf(path, sizeof(path), "%s/wal-inbox-manifest", directory);
	snprintf(temporary, sizeof(temporary), "%s/wal-inbox-manifest.tmp", directory);
	length = snprintf(manifest, sizeof(manifest),
					  "version=1\nsystem_identifier=" UINT64_FORMAT "\ntimeline=%u\nepoch=" UINT64_FORMAT
					  "\nstart=%X/%X\nend=%X/%X\nsegment_size=%u\n",
					  system_identifier, timeline, epoch, LSN_FORMAT_ARGS(start_lsn),
					  LSN_FORMAT_ARGS(end_lsn), segment_size);
	fd = open(temporary, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY, 0600);
	if (fd < 0)
		pg_fatal("could not create WAL bundle manifest: %m");
	write_all(fd, manifest, length);
	if (close(fd) != 0 || durable_rename(temporary, path) != 0)
		pg_fatal("could not sync WAL bundle manifest: %m");
	printf("%s", manifest);
	return 0;
}
