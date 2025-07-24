/*-------------------------------------------------------------------------
 *
 * pg_shmem_posix.c
 *	  POSIX-specific shared memory operations for libpg_shmem
 *
 * This file contains POSIX/Unix-specific functions for shared memory
 * operations including System V shared memory support.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/pg_shmem_posix.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#ifndef WIN32

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif

#include "libpg_shmem-fe.h"
#include "pg_shmem_internal.h"

/*
 * pg_shmem_attach_posix
 *
 * Attach to a System V shared memory segment on POSIX systems.
 */
PgShmemError
pg_shmem_attach_posix(unsigned long segment_id, void **address, size_t *size)
{
#ifdef HAVE_SYS_SHM_H
	struct shmid_ds shmstat;
	void	   *addr;
	int			shmid = (int) segment_id;

	if (!address || !size)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	*address = NULL;
	*size = 0;

	/* Get segment information first */
	printf("Debug: Attempting shmctl(IPC_STAT) on segment ID %d\n", shmid);
	fflush(stdout);
	
	if (shmctl(shmid, IPC_STAT, &shmstat) < 0)
	{
		printf("Debug: shmctl(IPC_STAT) failed with errno %d: %s\n", errno, strerror(errno));
		fflush(stdout);
		pg_shmem_set_error_context("shmctl(IPC_STAT) failed");
		
		switch (errno)
		{
			case EACCES:
				return PGSHMEM_ERROR_ACCESS_DENIED;
			case EINVAL:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			case EIDRM:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			default:
				return PGSHMEM_ERROR_ATTACH_FAILED;
		}
	}
	
	printf("Debug: shmctl(IPC_STAT) succeeded, segment size: %zu\n", (size_t)shmstat.shm_segsz);
	fflush(stdout);

	/* Attach to the segment with read-only access */
	printf("Debug: Attempting shmat() on segment ID %d with SHM_RDONLY\n", shmid);
	fflush(stdout);
	
	addr = shmat(shmid, NULL, SHM_RDONLY);
	if (addr == (void *) -1)
	{
		printf("Debug: shmat() failed with errno %d: %s\n", errno, strerror(errno));
		fflush(stdout);
		pg_shmem_set_error_context("shmat() failed");
		
		switch (errno)
		{
			case EACCES:
				return PGSHMEM_ERROR_ACCESS_DENIED;
			case EINVAL:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			case EIDRM:
				return PGSHMEM_ERROR_INVALID_SEGMENT;
			case ENOMEM:
				return PGSHMEM_ERROR_OUT_OF_MEMORY;
			default:
				return PGSHMEM_ERROR_ATTACH_FAILED;
		}
	}
	
	printf("Debug: shmat() succeeded, attached at address %p\n", addr);
	fflush(stdout);

	/* Success */
	*address = addr;
	*size = shmstat.shm_segsz;

	SHMEM_DEBUG_LOG(("Attached to SYSV shmem segment %lu at %p, size %zu",
					  segment_id, addr, *size));

	return PGSHMEM_OK;
#else
	pg_shmem_set_error_context("System V shared memory not supported");
	return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
}

/*
 * pg_shmem_detach_posix
 *
 * Detach from a System V shared memory segment on POSIX systems.
 */
PgShmemError
pg_shmem_detach_posix(void *address, size_t size)
{
#ifdef HAVE_SYS_SHM_H
	if (!address)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	if (shmdt(address) < 0)
	{
		pg_shmem_set_error_context("shmdt() failed");
		return PGSHMEM_ERROR_ATTACH_FAILED;
	}

	SHMEM_DEBUG_LOG(("Detached from SYSV shmem segment at %p", address));

	return PGSHMEM_OK;
#else
	pg_shmem_set_error_context("System V shared memory not supported");
	return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
}

/*
 * pg_shmem_get_segment_info_posix
 *
 * Get information about a System V shared memory segment.
 */
PgShmemError
pg_shmem_get_segment_info_posix(unsigned long segment_id, size_t *size, 
								 pid_t *creator_pid, int *nattach)
{
#ifdef HAVE_SYS_SHM_H
	struct shmid_ds shmstat;
	int			shmid = (int) segment_id;

	if (shmctl(shmid, IPC_STAT, &shmstat) < 0)
	{
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

	if (size)
		*size = shmstat.shm_segsz;
	if (creator_pid)
		*creator_pid = shmstat.shm_cpid;
	if (nattach)
		*nattach = shmstat.shm_nattch;

	return PGSHMEM_OK;
#else
	return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
}

/*
 * pg_shmem_list_segments_posix
 *
 * List all accessible System V shared memory segments.
 * This is mainly useful for debugging.
 */
PgShmemError
pg_shmem_list_segments_posix(int **segment_ids, int *count)
{
#ifdef HAVE_SYS_SHM_H
	/* This would require platform-specific code to enumerate all segments */
	/* For now, not implemented */
	if (segment_ids)
		*segment_ids = NULL;
	if (count)
		*count = 0;
	
	pg_shmem_set_error_context("Segment enumeration not implemented");
	return PGSHMEM_ERROR_UNKNOWN;
#else
	return PGSHMEM_ERROR_ATTACH_FAILED;
#endif
}

/*
 * Platform-specific utility functions
 */

/*
 * pg_shmem_check_sysv_support
 *
 * Check if System V shared memory is supported on this system.
 */
bool
pg_shmem_check_sysv_support(void)
{
#ifdef HAVE_SYS_SHM_H
	return true;
#else
	return false;
#endif
}

/*
 * pg_shmem_get_max_segment_size
 *
 * Get the maximum shared memory segment size on this system.
 */
size_t
pg_shmem_get_max_segment_size(void)
{
#ifdef HAVE_SYS_SHM_H
	/* Try to determine system limits */
	/* This is system-dependent and would need more complex implementation */
	return SIZE_MAX;  /* Placeholder */
#else
	return 0;
#endif
}

/*
 * pg_shmem_get_page_size
 *
 * Get the system page size.
 */
size_t
pg_shmem_get_page_size(void)
{
	long page_size = sysconf(_SC_PAGESIZE);
	if (page_size > 0)
		return (size_t) page_size;
	else
		return 4096;  /* Reasonable default */
}

#endif /* !WIN32 */ 