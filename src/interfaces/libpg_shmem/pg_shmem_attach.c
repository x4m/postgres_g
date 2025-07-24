/*-------------------------------------------------------------------------
 *
 * pg_shmem_attach.c
 *	  PostgreSQL Shared Memory Attachment Functions
 *
 * This file contains the core functionality for attaching to a running
 * PostgreSQL instance's shared memory segment.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/pg_shmem_attach.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>

#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif

/* Force System V IPC includes on macOS regardless of configure detection */
#ifdef __APPLE__
#ifndef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifndef HAVE_SYS_SHM_H
#include <sys/shm.h>
#define HAVE_SYS_SHM_H 1
#endif
#endif

/* We cannot include backend headers in frontend code, so we define what we need */
#include "libpg_shmem-fe.h"
#include "pg_shmem_internal.h"

/* Connection structure is defined in pg_shmem_internal.h */

/* Internal functions */
static PgShmemError find_postmaster_shmem_info(pid_t pid, unsigned long *segment_id, 
												char *datadir);
static PgShmemError attach_to_segment(unsigned long segment_id, void **address, 
									  size_t *size);
static PgShmemError validate_postgres_segment(void *address, size_t size, 
											  const char *datadir);
static PgShmemError detach_from_segment(void *address, size_t size);
static bool is_postmaster_alive(pid_t pid);

/*
 * PgShmemConnectByPid
 *
 * Connect to PostgreSQL shared memory using the postmaster PID.
 * This function:
 * 1. Validates that the PID is a PostgreSQL postmaster
 * 2. Finds the shared memory segment information
 * 3. Attaches to the shared memory segment
 * 4. Validates the segment contents
 * 5. Sets up access to the shmem index
 */
PgShmemConn *
PgShmemConnectByPid(pid_t postmaster_pid, PgShmemError *error)
{
	printf("DEBUG: PgShmemConnectByPid ENTRY - PID %d\n", (int)postmaster_pid);
	fflush(stdout);
	
	PgShmemConn *conn = NULL;
	PgShmemError	err = PGSHMEM_OK;
	unsigned long	segment_id;
	void		   *segment_address = NULL;
	size_t			segment_size;
	char			datadir[MAXPGPATH];

	/* Initialize error */
	if (error)
		*error = PGSHMEM_OK;

	printf("Debug: PgShmemConnectByPid called for PID %d\n", (int)postmaster_pid);

	/* Validate inputs */
	if (postmaster_pid <= 0)
	{
		printf("Debug: Invalid PID %d\n", (int)postmaster_pid);
		err = PGSHMEM_ERROR_INVALID_PID;
		goto error_exit;
	}

	/* Check if the process is alive and is a postmaster */
	printf("Debug: Checking if process is alive and is a postmaster\n");
	fflush(stdout);
	if (!is_postmaster_alive(postmaster_pid))
	{
		printf("Debug: is_postmaster_alive returned false\n");
		fflush(stdout);
		err = PGSHMEM_ERROR_INVALID_PID;
		goto error_exit;
	}
	printf("Debug: Process appears to be alive\n");
	fflush(stdout);

	/* Allocate connection structure */
	conn = (PgShmemConn *) malloc(sizeof(PgShmemConn));
	if (!conn)
	{
		err = PGSHMEM_ERROR_OUT_OF_MEMORY;
		goto error_exit;
	}
	memset(conn, 0, sizeof(PgShmemConn));

	/* Find the shared memory segment information */
	printf("Debug: About to call find_postmaster_shmem_info\n");
	fflush(stdout);
	err = find_postmaster_shmem_info(postmaster_pid, &segment_id, datadir);
	if (err != PGSHMEM_OK)
	{
		printf("Debug: find_postmaster_shmem_info returned error %d\n", err);
		fflush(stdout);
		goto error_exit;
	}
	printf("Debug: find_postmaster_shmem_info succeeded\n");
	fflush(stdout);

	/* Attach to the shared memory segment */
	printf("Debug: About to call attach_to_segment with segment_id=%lu\n", segment_id);
	fflush(stdout);
	
	err = attach_to_segment(segment_id, &segment_address, &segment_size);
	if (err != PGSHMEM_OK)
	{
		printf("Debug: attach_to_segment failed with error %d\n", err);
		fflush(stdout);
		goto error_exit;
	}
	
	printf("Debug: attach_to_segment succeeded, address=%p, size=%zu\n", segment_address, segment_size);
	fflush(stdout);

	/* Validate that this is a PostgreSQL segment */
	err = validate_postgres_segment(segment_address, segment_size, datadir);
	if (err != PGSHMEM_OK)
		goto error_exit;

	/* Initialize connection structure */
	conn->postmaster_pid = postmaster_pid;
	conn->segment_id = segment_id;
	conn->segment_address = segment_address;
	conn->segment_size = segment_size;
	conn->header = (PGShmemHeader *) segment_address;
	conn->shmem_index = conn->header->index;
	conn->attached = true;
	strncpy(conn->datadir, datadir, sizeof(conn->datadir) - 1);
	conn->datadir[sizeof(conn->datadir) - 1] = '\0';

	return conn;

error_exit:
	if (error)
		*error = err;
	
	if (segment_address)
		detach_from_segment(segment_address, segment_size);
	
	if (conn)
		free(conn);
	
	return NULL;
}

/*
 * PgShmemDisconnect
 *
 * Disconnect from PostgreSQL shared memory and free resources.
 */
void
PgShmemDisconnect(PgShmemConn *conn)
{
	if (!conn)
		return;

	if (conn->attached && conn->segment_address)
	{
		detach_from_segment(conn->segment_address, conn->segment_size);
	}

	memset(conn, 0, sizeof(PgShmemConn));
	free(conn);
}

/*
 * PgShmemGetConnInfo
 *
 * Get information about the current connection.
 */
PgShmemError
PgShmemGetConnInfo(PgShmemConn *conn, PgShmemConnInfo *info)
{
	if (!conn || !conn->attached || !info)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	memset(info, 0, sizeof(PgShmemConnInfo));
	
	info->postmaster_pid = conn->postmaster_pid;
	info->segment_size = conn->segment_size;
	info->segment_id = conn->segment_id;
	info->segment_address = conn->segment_address;
	
	/* Get PostgreSQL version and system identifier from control file */
	if (conn->header)
	{
		/* We'll need to implement this based on the control file structure */
		info->pg_version = 0;  /* TODO: Extract from control file */
		info->system_identifier = 0;  /* TODO: Extract from control file */
	}

	return PGSHMEM_OK;
}

/*
 * PgShmemConnectionValid
 *
 * Check if the connection is still valid.
 */
bool
PgShmemConnectionValid(PgShmemConn *conn)
{
	if (!conn || !conn->attached)
		return false;

	/* Check if postmaster is still alive */
	if (!is_postmaster_alive(conn->postmaster_pid))
		return false;

	/* Check if shared memory is still valid */
	if (!conn->segment_address || !conn->header)
		return false;

	/* Check magic number */
	if (conn->header->magic != PGShmemMagic)
		return false;

	return true;
}

/*
 * PgShmemPostmasterAlive
 *
 * Check if the PostgreSQL postmaster is still alive.
 */
bool
PgShmemPostmasterAlive(PgShmemConn *conn)
{
	if (!conn)
		return false;

	return is_postmaster_alive(conn->postmaster_pid);
}

/*
 * find_postmaster_shmem_info
 *
 * Find shared memory information for a PostgreSQL postmaster.
 * This reads the data directory lock file to get shared memory details.
 */
static PgShmemError
find_postmaster_shmem_info(pid_t pid, unsigned long *segment_id, char *datadir)
{
	char		lockfile_path[MAXPGPATH];
	FILE	   *fp;
	char		line[1024];
	int			line_num = 0;
	pid_t		lock_pid = 0;
	unsigned long shmem_key = 0;
	unsigned long shmem_id = 0;
	
	printf("Debug: find_postmaster_shmem_info called for PID %d\n", (int)pid);
	fflush(stdout);
	
	/* Find the data directory for this process */
	/* This is a simplified approach - in reality, we'd need to parse
	 * the process command line or use other methods to find the data dir */
	printf("Debug: About to call pg_shmem_find_datadir_by_pid\n");
	fflush(stdout);
	if (!pg_shmem_find_datadir_by_pid(pid, datadir))
	{
		printf("Debug: pg_shmem_find_datadir_by_pid returned false\n");
		fflush(stdout);
		return PGSHMEM_ERROR_NOT_POSTGRES;
	}
	printf("Debug: pg_shmem_find_datadir_by_pid succeeded, datadir: %s\n", datadir);
	fflush(stdout);

	/* Build path to postmaster.pid file */
	snprintf(lockfile_path, sizeof(lockfile_path), "%s/postmaster.pid", datadir);

	/* Open the lock file */
	fp = fopen(lockfile_path, "r");
	if (!fp)
		return PGSHMEM_ERROR_NOT_POSTGRES;

	/* Parse the lock file */
	while (fgets(line, sizeof(line), fp) && line_num < 10)
	{
		line_num++;
		printf("Debug: postmaster.pid line %d: %s", line_num, line);
		fflush(stdout);
		
		switch (line_num)
		{
			case 1:
				/* First line is PID */
				lock_pid = (pid_t) strtol(line, NULL, 10);
				printf("Debug: Parsed PID: %d\n", (int)lock_pid);
				fflush(stdout);
				break;
			case 7:
				/* Seventh line contains shared memory segment ID and key */
				if (sscanf(line, "%lu %lu", &shmem_key, &shmem_id) == 2)
				{
					*segment_id = shmem_id;
					printf("Debug: Parsed shared memory ID: %lu, key: %lu\n", shmem_id, shmem_key);
					fflush(stdout);
				}
				else
				{
					printf("Debug: Failed to parse shared memory info from line: %s", line);
					fflush(stdout);
				}
				break;
		}
	}

	fclose(fp);

	printf("Debug: Finished parsing postmaster.pid, lock_pid=%d, expected_pid=%d\n", (int)lock_pid, (int)pid);
	fflush(stdout);

	/* Verify this is the right process */
	if (lock_pid != pid)
	{
		printf("Debug: PID mismatch in postmaster.pid\n");
		fflush(stdout);
		return PGSHMEM_ERROR_INVALID_PID;
	}

	if (shmem_id == 0)
	{
		printf("Debug: No shared memory ID found in postmaster.pid\n");
		fflush(stdout);
		return PGSHMEM_ERROR_NOT_POSTGRES;
	}

	printf("Debug: Successfully extracted shared memory ID: %lu\n", shmem_id);
	fflush(stdout);

	return PGSHMEM_OK;
}

/*
 * attach_to_segment
 *
 * Attach to a System V shared memory segment.
 */
static PgShmemError
attach_to_segment(unsigned long segment_id, void **address, size_t *size)
{
	printf("Debug: attach_to_segment entry, checking HAVE_SYS_SHM_H\n");
	fflush(stdout);
	
#ifdef HAVE_SYS_SHM_H
	printf("Debug: HAVE_SYS_SHM_H is defined, proceeding with System V IPC\n");
	fflush(stdout);
	struct shmid_ds shmstat;
	void	   *addr;

	printf("Debug: attach_to_segment called with segment_id=%lu\n", segment_id);
	fflush(stdout);

	/* Get segment information */
	printf("Debug: Calling shmctl(IPC_STAT) on segment %d\n", (int)segment_id);
	fflush(stdout);
	
	if (shmctl((int) segment_id, IPC_STAT, &shmstat) < 0)
	{
		printf("Debug: shmctl(IPC_STAT) failed with errno %d: %s\n", errno, strerror(errno));
		fflush(stdout);
		
		switch (errno)
		{
			case EACCES:
				return PGSHMEM_ERROR_ACCESS_DENIED;
			case EINVAL:
			case EIDRM:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			default:
				return PGSHMEM_ERROR_ATTACH_FAILED;
		}
	}

	printf("Debug: shmctl(IPC_STAT) succeeded, segment size: %zu\n", (size_t)shmstat.shm_segsz);
	fflush(stdout);

	/* Attach to the segment */
	printf("Debug: Calling shmat() on segment %d with SHM_RDONLY\n", (int)segment_id);
	fflush(stdout);
	
	addr = shmat((int) segment_id, NULL, SHM_RDONLY);
	if (addr == (void *) -1)
	{
		printf("Debug: shmat() failed with errno %d: %s\n", errno, strerror(errno));
		fflush(stdout);
		
		switch (errno)
		{
			case EACCES:
				return PGSHMEM_ERROR_ACCESS_DENIED;
			case EINVAL:
			case EIDRM:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			default:
				return PGSHMEM_ERROR_ATTACH_FAILED;
		}
	}

	printf("Debug: shmat() succeeded, attached at address %p, size %zu\n", addr, (size_t)shmstat.shm_segsz);
	fflush(stdout);

	*address = addr;
	*size = shmstat.shm_segsz;
	return PGSHMEM_OK;
#else
	printf("Debug: HAVE_SYS_SHM_H is NOT defined, returning PGSHMEM_ERROR_ATTACH_FAILED\n");
	fflush(stdout);
	return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
}

/*
 * validate_postgres_segment
 *
 * Validate that the attached segment belongs to PostgreSQL and matches
 * the expected data directory.
 */
static PgShmemError
validate_postgres_segment(void *address, size_t size, const char *datadir)
{
	PGShmemHeader *header = (PGShmemHeader *) address;
	struct stat statbuf;

	/* Check minimum size */
	if (size < sizeof(PGShmemHeader))
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	/* Check magic number */
	if (header->magic != PGShmemMagic)
		return PGSHMEM_ERROR_MAGIC_MISMATCH;

	/* Check data directory matches */
	if (stat(datadir, &statbuf) == 0)
	{
#ifndef WIN32
		if (header->device != statbuf.st_dev || header->inode != statbuf.st_ino)
			return PGSHMEM_ERROR_INVALID_SEGMENT;
#endif
	}

	return PGSHMEM_OK;
}

/*
 * detach_from_segment
 *
 * Detach from a shared memory segment.
 */
static PgShmemError
detach_from_segment(void *address, size_t size)
{
#ifdef HAVE_SYS_SHM_H
	if (shmdt(address) < 0)
		return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
	return PGSHMEM_OK;
}

/*
 * is_postmaster_alive
 *
 * Check if a process is still alive.
 */
static bool
is_postmaster_alive(pid_t pid)
{
	if (pid <= 0)
		return false;

	/* Try to send signal 0 to check if process exists */
	if (kill(pid, 0) == 0)
		return true;

	/* If errno is EPERM, process exists but we can't signal it */
	if (errno == EPERM)
		return true;

	return false;
} 