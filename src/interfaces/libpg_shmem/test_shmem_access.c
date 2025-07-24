/*-------------------------------------------------------------------------
 *
 * test_shmem_access.c
 *	  Test program for PostgreSQL Shared Memory Access Library
 *
 * This program demonstrates how to use libpg_shmem to connect to a running
 * PostgreSQL instance and access its shared memory structures.
 *
 * Usage:
 *   test_shmem_access <postmaster_pid>
 *
 * Example:
 *   test_shmem_access 12345
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/test_shmem_access.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>

#include "libpg_shmem-fe.h"

static void usage(const char *progname);
static void print_connection_info(PgShmemConn *conn);
static void print_structure_info(PgShmemConn *conn, const char *struct_name);
static void test_xlog_ctl_access(PgShmemConn *conn);

int
main(int argc, char *argv[])
{
	pid_t		postmaster_pid;
	PgShmemConn *conn;
	PgShmemError error;
	char	   *endptr;

	/* Parse command line arguments */
	if (argc != 2)
	{
		usage(argv[0]);
		exit(1);
	}

	postmaster_pid = (pid_t) strtol(argv[1], &endptr, 10);
	if (*endptr != '\0' || postmaster_pid <= 0)
	{
		fprintf(stderr, "Error: Invalid PID '%s'\n", argv[1]);
		usage(argv[0]);
		exit(1);
	}

	printf("PostgreSQL Shared Memory Access Test\n");
	printf("=====================================\n\n");

	/* Check if the PID is a PostgreSQL postmaster */
	printf("Checking if PID %d is a PostgreSQL postmaster...\n", postmaster_pid);

	/* Add debug output */
	printf("Debug: About to call PgShmemConnectByPid...\n");

	/* Connect to PostgreSQL shared memory */
	conn = PgShmemConnectByPid(postmaster_pid, &error);
	if (!conn)
	{
		fprintf(stderr, "Error: Failed to connect to shared memory: %s\n",
				PgShmemErrorMessage(error));
		
		/* Add detailed debug information */
		printf("Debug: Connection failed with error code: %d\n", error);
		printf("Debug: Error message: %s\n", PgShmemErrorMessage(error));
		
		exit(1);
	}
	printf("Successfully connected to PostgreSQL shared memory\n\n");

	/* Print connection information */
	print_connection_info(conn);

	/* Test structure access */
	printf("Testing structure access...\n");
	print_structure_info(conn, "XLOG Ctl");
	print_structure_info(conn, "Control File");
	print_structure_info(conn, "Proc Array");

	/* Test XLOG Ctl access specifically */
	test_xlog_ctl_access(conn);

	/* Check if connection is still valid */
	printf("Connection validation...\n");
	if (PgShmemConnectionValid(conn))
		printf("✓ Connection is still valid\n");
	else
		printf("✗ Connection is no longer valid\n");

	if (PgShmemPostmasterAlive(conn))
		printf("✓ PostgreSQL postmaster is still running\n");
	else
		printf("✗ PostgreSQL postmaster is no longer running\n");

	printf("\n");

	/* Disconnect */
	printf("Disconnecting from shared memory...\n");
	PgShmemDisconnect(conn);
	printf("✓ Disconnected successfully\n");

	printf("\nTest completed successfully!\n");
	return 0;
}

static void
usage(const char *progname)
{
	printf("Usage: %s <postmaster_pid>\n", progname);
	printf("\n");
	printf("Connect to PostgreSQL shared memory and access internal structures.\n");
	printf("\n");
	printf("Arguments:\n");
	printf("  postmaster_pid    PID of the PostgreSQL postmaster process\n");
	printf("\n");
	printf("Example:\n");
	printf("  %s 12345\n", progname);
	printf("\n");
	printf("Note: This program requires read access to PostgreSQL's shared memory.\n");
	printf("You may need to run it as the same user as PostgreSQL or with\n");
	printf("appropriate permissions.\n");
}

static void
print_connection_info(PgShmemConn *conn)
{
	PgShmemConnInfo info;
	PgShmemError error;

	error = PgShmemGetConnInfo(conn, &info);
	if (error != PGSHMEM_OK)
	{
		printf("Error getting connection info: %s\n", PgShmemErrorMessage(error));
		return;
	}

	printf("Connection Information:\n");
	printf("  Postmaster PID:     %d\n", info.postmaster_pid);
	printf("  Segment Size:       %zu bytes (%.2f MB)\n", 
		   info.segment_size, (double) info.segment_size / (1024 * 1024));
	printf("  Segment ID:         %lu\n", info.segment_id);
	printf("  Segment Address:    %p\n", info.segment_address);
	printf("  PostgreSQL Version: %u\n", info.pg_version);
	printf("  System Identifier:  %lu\n", (unsigned long) info.system_identifier);
	printf("\n");
}

static void
print_structure_info(PgShmemConn *conn, const char *struct_name)
{
	PgShmemStructInfo info;
	PgShmemError error;

	printf("Structure: %s\n", struct_name);

	error = PgShmemGetStructInfo(conn, struct_name, &info);
	if (error == PGSHMEM_OK)
	{
		printf("  Address:        %p\n", info.address);
		printf("  Size:           %zu bytes\n", info.size);
		printf("  Allocated Size: %zu bytes\n", info.allocated_size);
		printf("  Status:         Found\n");
	}
	else
	{
		printf("  Status:         %s\n", PgShmemErrorMessage(error));
	}
	printf("\n");
}

static void
test_xlog_ctl_access(PgShmemConn *conn)
{
	void *xlog_ctl;
	PgShmemError error;
	size_t size;

	printf("Testing XLOG Ctl structure access...\n");

	/* Try to get the XLOG Ctl structure */
	xlog_ctl = PgShmemGetXLogCtl(conn, &error);
	if (xlog_ctl)
	{
		printf("XLOG Ctl structure found at address: %p\n", xlog_ctl);

		/* Try to get a safe copy */
		void *copy = PgShmemGetStructCopy(conn, "XLOG Ctl", &size, &error);
		if (copy)
		{
			printf("✓ Successfully copied XLOG Ctl structure (%zu bytes)\n", size);
			PgShmemFreeCopy(copy);
		}
		else
		{
			printf("✗ Failed to copy XLOG Ctl structure: %s\n", 
				   PgShmemErrorMessage(error));
		}
	}
	else
	{
		printf("✗ XLOG Ctl structure not found: %s\n", PgShmemErrorMessage(error));
	}
	printf("\n");
} 