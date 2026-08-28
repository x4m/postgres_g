/*
 * libpq_testclient.c
 *		A test program for the libpq public API
 *
 * Copyright (c) 2022-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/interfaces/libpq/test/libpq_testclient.c
 */

#include "postgres_fe.h"

#include "libpq-fe.h"

static void
print_ssl_library(void)
{
	const char *lib = PQsslAttribute(NULL, "library");

	if (!lib)
		fprintf(stderr, "SSL is not enabled\n");
	else
		printf("%s\n", lib);
}

static int
test_compression_reset(const char *conninfo)
{
	PGconn	   *conn = PQconnectdb(conninfo);
	PGresult   *res;

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQfinish(conn);
		return 1;
	}
	res = PQexec(conn, "SELECT repeat('x', 17 * 1024 * 1024)");
	if (PQresultStatus(res) != PGRES_TUPLES_OK ||
		PQgetlength(res, 0, 0) != 17 * 1024 * 1024)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	PQclear(res);
	PQreset(conn);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQfinish(conn);
		return 1;
	}
	res = PQexec(conn, "SELECT repeat('after reset ', 1000)");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	PQclear(res);
	PQfinish(conn);
	return 0;
}

static int
test_compression_large_copy(const char *conninfo)
{
	const int	data_size = 17 * 1024 * 1024;
	PGconn	   *conn = PQconnectdb(conninfo);
	PGresult   *res;
	char	   *data;
	int			status = 1;

	if (PQstatus(conn) != CONNECTION_OK)
		goto fail;
	res = PQexec(conn, "CREATE TEMP TABLE compression_copy (data text); "
				 "COPY compression_copy FROM STDIN");
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		PQclear(res);
		goto fail;
	}
	PQclear(res);

	data = malloc(data_size + 1);
	if (data == NULL)
		goto fail;
	memset(data, 'x', data_size);
	data[data_size] = '\n';
	if (PQputCopyData(conn, data, data_size + 1) != 1 ||
		PQputCopyEnd(conn, NULL) != 1)
	{
		free(data);
		goto fail;
	}
	free(data);
	while ((res = PQgetResult(conn)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQclear(res);
			goto fail;
		}
		PQclear(res);
	}

	res = PQexec(conn, "SELECT octet_length(data) FROM compression_copy");
	if (PQresultStatus(res) == PGRES_TUPLES_OK &&
		!strcmp(PQgetvalue(res, 0, 0), "17825792"))
		status = 0;
	PQclear(res);

fail:
	if (status != 0)
		fprintf(stderr, "%s", PQerrorMessage(conn));
	PQfinish(conn);
	return status;
}

int
main(int argc, char *argv[])
{
	if ((argc > 1) && !strcmp(argv[1], "--ssl"))
	{
		print_ssl_library();
		return 0;
	}
	if (argc == 3 && !strcmp(argv[1], "--compression-reset"))
		return test_compression_reset(argv[2]);
	if (argc == 3 && !strcmp(argv[1], "--compression-large-copy"))
		return test_compression_large_copy(argv[2]);

	printf("supported arguments are --ssl, --compression-reset CONNINFO, "
		   "and --compression-large-copy CONNINFO\n");
	return 1;
}
