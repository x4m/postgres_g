/*-------------------------------------------------------------------------
 *
 * pg_shmem_utils.c
 *	  PostgreSQL Shared Memory Access Library - Utility Functions
 *
 * This file contains utility functions for finding PostgreSQL processes,
 * data directories, and other helper functions.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/pg_shmem_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>

#ifdef HAVE_SYS_PROCFS_H
#include <sys/procfs.h>
#endif

#if defined(__APPLE__) || defined(__Darwin__)
#include <sys/sysctl.h>
#include <sys/proc.h>
#include <libproc.h>
#endif

#include "libpg_shmem-fe.h"
#include "pg_shmem_internal.h"

/* Platform-specific includes */
#ifdef __linux__
#include <sys/proc.h>
#endif

#ifdef __Darwin__
#include <libproc.h>
#endif

/* Internal function declarations */
static bool read_proc_cmdline(pid_t pid, char *cmdline, size_t cmdline_size);
static bool parse_postgres_cmdline(const char *cmdline, char *datadir);
static bool validate_postgres_datadir(const char *datadir);
static bool pg_shmem_search_for_postmaster_pid(pid_t target_pid, char *datadir);
static bool pg_shmem_check_postmaster_pid_in_dir(const char *dir_path, pid_t target_pid, char *datadir);
static bool pg_shmem_recursive_search_postmaster_pid(const char *base_path, pid_t target_pid, char *datadir, int max_depth);

/*
 * PgShmemIsPostmasterPid
 *
 * Check if a PID is a PostgreSQL postmaster process.
 */
bool
PgShmemIsPostmasterPid(pid_t pid)
{
	char cmdline[1024];
	char datadir[MAXPGPATH];

	if (pid <= 0)
		return false;

	/* Read the process command line */
	if (!read_proc_cmdline(pid, cmdline, sizeof(cmdline)))
		return false;

	/* Check if it looks like a PostgreSQL process */
	if (strstr(cmdline, "postgres") == NULL && strstr(cmdline, "postmaster") == NULL)
		return false;

	/* Try to extract the data directory */
	if (!parse_postgres_cmdline(cmdline, datadir))
		return false;

	/* Validate that it's a real PostgreSQL data directory */
	if (!validate_postgres_datadir(datadir))
		return false;

	return true;
}

/*
 * PgShmemGetSegmentId
 *
 * Get the shared memory segment ID for a PostgreSQL process.
 */
PgShmemError
PgShmemGetSegmentId(pid_t postmaster_pid, unsigned long *segment_id)
{
	char datadir[MAXPGPATH];
	char lockfile_path[MAXPGPATH];
	FILE *fp;
	char line[1024];
	int line_num = 0;
	pid_t lock_pid = 0;
	unsigned long shmem_key = 0;
	unsigned long shmem_id = 0;

	if (!segment_id)
		return PGSHMEM_ERROR_INVALID_PID;

	*segment_id = 0;

	/* Find the data directory */
	if (!pg_shmem_find_datadir_by_pid(postmaster_pid, datadir))
		return PGSHMEM_ERROR_NOT_POSTGRES;

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

		switch (line_num)
		{
			case 1:
				/* First line is PID */
				lock_pid = (pid_t) strtol(line, NULL, 10);
				break;
			case 6:
				/* Sixth line contains shared memory key and ID */
				if (sscanf(line, "%lu %lu", &shmem_key, &shmem_id) == 2)
				{
					*segment_id = shmem_id;
				}
				break;
		}
	}

	fclose(fp);

	/* Verify this is the right process */
	if (lock_pid != postmaster_pid)
		return PGSHMEM_ERROR_INVALID_PID;

	if (shmem_id == 0)
		return PGSHMEM_ERROR_NOT_POSTGRES;

	return PGSHMEM_OK;
}

/*
 * PgShmemValidateSegment
 *
 * Validate that a shared memory segment belongs to PostgreSQL.
 */
PgShmemError
PgShmemValidateSegment(void *segment_addr, size_t segment_size)
{
	PGShmemHeader *header;

	if (!segment_addr || segment_size < sizeof(PGShmemHeader))
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	header = (PGShmemHeader *) segment_addr;

	/* Check magic number */
	if (header->magic != PGShmemMagic)
		return PGSHMEM_ERROR_MAGIC_MISMATCH;

	/* Check segment size consistency */
	if (header->totalsize != segment_size)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	return PGSHMEM_OK;
}

/*
 * pg_shmem_find_datadir_by_pid
 *
 * Find the PostgreSQL data directory for a given PID.
 */
bool
pg_shmem_find_datadir_by_pid(pid_t pid, char *datadir)
{
	printf("Debug: pg_shmem_find_datadir_by_pid called for PID %d\n", (int)pid);

	if (!datadir)
		return false;

	/* Strategy: Find postmaster.pid files and check if they match our PID */
	/* This is much more reliable than trying to parse command lines */
	
	return pg_shmem_search_for_postmaster_pid(pid, datadir);
}

/*
 * pg_shmem_search_for_postmaster_pid
 *
 * Search for a postmaster.pid file that contains the given PID.
 * This is more reliable than trying to parse command lines.
 */
static bool
pg_shmem_search_for_postmaster_pid(pid_t target_pid, char *datadir)
{
	/* Check for environment variable with explicit data directory (for testing) */
	const char *env_datadir = getenv("PGDATA_FOR_TEST");
	if (env_datadir && pg_shmem_check_postmaster_pid_in_dir(env_datadir, target_pid, datadir))
	{
		printf("Debug: Found matching postmaster.pid via PGDATA_FOR_TEST env var: %s\n", datadir);
		return true;
	}

	char *search_paths[] = {
		".",                                    /* Current directory */
		"..",                                   /* Parent directory */
		"./tmp_check",                          /* PostgreSQL test framework - current */
		"../tmp_check",                         /* PostgreSQL test framework - parent */
		"./tmp_check/t_001_basic_shmem_access_shmem_test_data/pgdata", /* TAP test specific */
		"/tmp",                                 /* Common temp location */
		"/Users/x4mmm/project/bin/testdb",      /* User's test database */
		"/var/lib/postgresql/data",             /* Debian/Ubuntu default */
		"/usr/local/var/postgres",              /* Homebrew default */
		"/opt/homebrew/var/postgres",           /* Apple Silicon Homebrew */
		"/usr/local/pgsql/data",                /* Source install default */
		"/opt/local/var/db/postgresql/data",    /* MacPorts */
		NULL
	};

	printf("Debug: Searching for postmaster.pid with PID %d\n", (int)target_pid);

	/* First, search common locations */
	for (int i = 0; search_paths[i] != NULL; i++)
	{
		if (pg_shmem_check_postmaster_pid_in_dir(search_paths[i], target_pid, datadir))
		{
			printf("Debug: Found matching postmaster.pid in %s\n", datadir);
			return true;
		}
	}

	/* If not found in common locations, search the current working directory tree */
	if (pg_shmem_recursive_search_postmaster_pid(".", target_pid, datadir, 6))
	{
		printf("Debug: Found matching postmaster.pid via recursive search in %s\n", datadir);
		return true;
	}

	/* Search /tmp recursively for test frameworks */
	if (pg_shmem_recursive_search_postmaster_pid("/tmp", target_pid, datadir, 3))
	{
		printf("Debug: Found matching postmaster.pid in /tmp tree: %s\n", datadir);
		return true;
	}

	/* Search parent directories for build environments */
	if (pg_shmem_recursive_search_postmaster_pid("..", target_pid, datadir, 3))
	{
		printf("Debug: Found matching postmaster.pid in parent tree: %s\n", datadir);
		return true;
	}

	/* Search build directory structures */
	if (pg_shmem_recursive_search_postmaster_pid("../../..", target_pid, datadir, 4))
	{
		printf("Debug: Found matching postmaster.pid in build tree: %s\n", datadir);
		return true;
	}

	/* Search tmp_install directory (test framework) */
	if (pg_shmem_recursive_search_postmaster_pid("../../../tmp_install", target_pid, datadir, 3))
	{
		printf("Debug: Found matching postmaster.pid in tmp_install: %s\n", datadir);
		return true;
	}

	printf("Debug: Could not find postmaster.pid for PID %d\n", (int)target_pid);
	return false;
}

/*
 * pg_shmem_check_postmaster_pid_in_dir
 *
 * Check if a specific directory contains a postmaster.pid with the target PID.
 */
static bool
pg_shmem_check_postmaster_pid_in_dir(const char *dir_path, pid_t target_pid, char *datadir)
{
	char pid_file_path[MAXPGPATH];
	FILE *fp;
	char line[256];
	pid_t file_pid;

	snprintf(pid_file_path, sizeof(pid_file_path), "%s/postmaster.pid", dir_path);
	
	fp = fopen(pid_file_path, "r");
	if (!fp)
		return false;

	/* Read first line which contains the PID */
	if (fgets(line, sizeof(line), fp))
	{
		file_pid = (pid_t)strtol(line, NULL, 10);
		if (file_pid == target_pid)
		{
			/* Found matching PID, copy the directory path */
			strncpy(datadir, dir_path, MAXPGPATH - 1);
			datadir[MAXPGPATH - 1] = '\0';
			fclose(fp);
			return true;
		}
	}
	
	fclose(fp);
	return false;
}

/*
 * pg_shmem_recursive_search_postmaster_pid
 *
 * Recursively search for postmaster.pid files in directory tree.
 */
static bool
pg_shmem_recursive_search_postmaster_pid(const char *base_path, pid_t target_pid, char *datadir, int max_depth)
{
	DIR *dir;
	struct dirent *entry;
	char full_path[MAXPGPATH];
	struct stat statbuf;

	if (max_depth <= 0)
		return false;

	/* First check if this directory has a matching postmaster.pid */
	if (pg_shmem_check_postmaster_pid_in_dir(base_path, target_pid, datadir))
		return true;

	/* Then search subdirectories */
	dir = opendir(base_path);
	if (!dir)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip hidden directories and current/parent references */
		if (entry->d_name[0] == '.')
			continue;

		snprintf(full_path, sizeof(full_path), "%s/%s", base_path, entry->d_name);
		
		if (stat(full_path, &statbuf) == 0 && S_ISDIR(statbuf.st_mode))
		{
			if (pg_shmem_recursive_search_postmaster_pid(full_path, target_pid, datadir, max_depth - 1))
			{
				closedir(dir);
				return true;
			}
		}
	}

	closedir(dir);
	return false;
}

/*
 * pg_shmem_check_postgres_process
 *
 * Check if a process appears to be PostgreSQL.
 */
bool
pg_shmem_check_postgres_process(pid_t pid)
{
	char cmdline[1024];

	if (pid <= 0)
		return false;

	/* Read the process command line */
	if (!read_proc_cmdline(pid, cmdline, sizeof(cmdline)))
		return false;

	/* Check if it contains postgres-related strings */
	return (strstr(cmdline, "postgres") != NULL || 
			strstr(cmdline, "postmaster") != NULL);
}

/*
 * Memory validation functions
 */

/*
 * pg_shmem_validate_pointer
 *
 * Validate that a pointer can be safely accessed.
 */
bool
pg_shmem_validate_pointer(PgShmemConn *conn, const void *ptr, size_t size)
{
	if (!conn || !conn->attached || !ptr)
		return false;

	return pg_shmem_addr_in_segment(conn, ptr) &&
		   pg_shmem_addr_in_segment(conn, (char *) ptr + size - 1);
}

/*
 * pg_shmem_addr_in_segment
 *
 * Check if an address is within the shared memory segment.
 */
bool
pg_shmem_addr_in_segment(PgShmemConn *conn, const void *addr)
{
	if (!conn || !conn->attached || !addr)
		return false;

	return (addr >= conn->segment_address &&
			addr < (void *) ((char *) conn->segment_address + conn->segment_size));
}

/*
 * Connection state management
 */

/*
 * pg_shmem_conn_is_valid
 *
 * Check if a connection is in a valid state.
 */
bool
pg_shmem_conn_is_valid(PgShmemConn *conn)
{
	if (!conn || !conn->attached)
		return false;

	if (!conn->segment_address || !conn->header)
		return false;

	/* Check magic number */
	if (conn->header->magic != PGShmemMagic)
		return false;

	return true;
}

/*
 * pg_shmem_invalidate_conn
 *
 * Mark a connection as invalid.
 */
void
pg_shmem_invalidate_conn(PgShmemConn *conn)
{
	if (conn)
		conn->attached = false;
}

/*
 * Platform-specific implementations
 */

/*
 * read_proc_cmdline
 *
 * Read the command line for a process.
 */
static bool
read_proc_cmdline(pid_t pid, char *cmdline, size_t cmdline_size)
{
	printf("Debug: read_proc_cmdline called for PID %d\n", (int)pid);
	
	if (!cmdline || cmdline_size == 0)
	{
		printf("Debug: Invalid parameters to read_proc_cmdline\n");
		return false;
	}

	cmdline[0] = '\0';

#ifdef __linux__
	{
		char proc_path[64];
		FILE *fp;
		size_t bytes_read;

		snprintf(proc_path, sizeof(proc_path), "/proc/%d/cmdline", (int) pid);
		
		fp = fopen(proc_path, "r");
		if (!fp)
			return false;

		bytes_read = fread(cmdline, 1, cmdline_size - 1, fp);
		fclose(fp);

		if (bytes_read == 0)
			return false;

		cmdline[bytes_read] = '\0';

		/* Convert null separators to spaces */
		for (size_t i = 0; i < bytes_read; i++)
		{
			if (cmdline[i] == '\0')
				cmdline[i] = ' ';
		}

		return true;
	}
#elif defined(__APPLE__) || defined(__Darwin__)
	{
		int mib[3];
		size_t size;
		char *procargs, *cp;
		int argc;

		printf("Debug: Using macOS sysctl to read command line\n");

		/* Set up sysctl path for process arguments */
		mib[0] = CTL_KERN;
		mib[1] = KERN_PROCARGS2;
		mib[2] = pid;

		/* Get size of process arguments */
		size = 0;
		if (sysctl(mib, 3, NULL, &size, NULL, 0) == -1)
		{
			printf("Debug: sysctl failed to get size: %s\n", strerror(errno));
			return false;
		}
		
		printf("Debug: Process args size: %zu\n", size);

		/* Allocate buffer for process arguments */
		procargs = malloc(size);
		if (procargs == NULL)
			return false;

		/* Get process arguments */
		if (sysctl(mib, 3, procargs, &size, NULL, 0) == -1) {
			free(procargs);
			return false;
		}

		/* First 4 bytes contain argc */
		memcpy(&argc, procargs, sizeof(argc));
		
		/* Skip past argc to get to actual args */
		cp = procargs + sizeof(argc);

		/* Skip the exec path (first string) */
		cp = cp + strlen(cp) + 1;

		/* Skip any null bytes */
		while (cp < procargs + size && *cp == '\0')
			cp++;

		/* Build command line by concatenating arguments */
		cmdline[0] = '\0';
		for (int i = 0; i < argc && cp < procargs + size; i++)
		{
			if (i > 0)
				strncat(cmdline, " ", cmdline_size - strlen(cmdline) - 1);
			strncat(cmdline, cp, cmdline_size - strlen(cmdline) - 1);
			cp += strlen(cp) + 1;
		}

		free(procargs);
		return (strlen(cmdline) > 0);
	}
#else
	/* Fallback for other platforms */
	return false;
#endif
}

/*
 * parse_postgres_cmdline
 *
 * Parse a PostgreSQL command line to extract the data directory.
 */
static bool
parse_postgres_cmdline(const char *cmdline, char *datadir)
{
	const char *p;
	const char *datadir_arg = "-D";
	const char *start, *end;

	if (!cmdline || !datadir)
		return false;

	/* Look for -D option */
	p = strstr(cmdline, datadir_arg);
	if (!p)
		return false;

	/* Skip past -D */
	p += 2;

	/* Skip whitespace */
	while (*p && (*p == ' ' || *p == '\t'))
		p++;

	if (!*p)
		return false;

	start = p;

	/* Find end of data directory path */
	while (*p && *p != ' ' && *p != '\t' && *p != '\0')
		p++;

	end = p;

	/* Copy the data directory path */
	{
		size_t len = end - start;
	if (len >= MAXPGPATH)
		return false;

		strncpy(datadir, start, len);
		datadir[len] = '\0';
	}

	return true;
}

/*
 * validate_postgres_datadir
 *
 * Validate that a directory is a PostgreSQL data directory.
 */
static bool
validate_postgres_datadir(const char *datadir)
{
	char pg_version_path[MAXPGPATH];
	char postmaster_pid_path[MAXPGPATH];
	struct stat statbuf;

	if (!datadir || !*datadir)
		return false;

	/* Check if directory exists */
	if (stat(datadir, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
		return false;

	/* Check for PG_VERSION file */
	snprintf(pg_version_path, sizeof(pg_version_path), "%s/PG_VERSION", datadir);
	if (stat(pg_version_path, &statbuf) != 0)
		return false;

	/* Check for postmaster.pid file (optional, as server might not be running) */
	snprintf(postmaster_pid_path, sizeof(postmaster_pid_path), "%s/postmaster.pid", datadir);
	/* We don't require this file to exist */

	return true;
}

/*
 * Debug and logging functions
 */
#ifdef PG_SHMEM_DEBUG
void
pg_shmem_debug_log(const char *fmt, ...)
{
	va_list args;
	char buffer[1024];
	
	va_start(args, fmt);
	vsnprintf(buffer, sizeof(buffer), fmt, args);
	va_end(args);
	
	fprintf(stderr, "[libpg_shmem DEBUG] %s\n", buffer);
}
#endif 