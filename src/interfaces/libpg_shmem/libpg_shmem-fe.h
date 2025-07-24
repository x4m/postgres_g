/*-------------------------------------------------------------------------
 *
 * libpg_shmem-fe.h
 *	  PostgreSQL Shared Memory Access Library
 *	  Frontend Interface Definitions
 *
 * This library allows external processes to attach to a running PostgreSQL
 * instance's shared memory segments and access internal data structures.
 * 
 * WARNING: This is an advanced and potentially dangerous operation.
 * - Only use this library if you understand PostgreSQL internals
 * - Ensure proper synchronization when reading shared data
 * - Be aware that data structures may change between PostgreSQL versions
 * - Improper usage can crash both your process and PostgreSQL
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/libpg_shmem-fe.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LIBPG_SHMEM_FE_H
#define LIBPG_SHMEM_FE_H

#ifdef __cplusplus
extern "C"
{
#endif

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>

/* PostgreSQL type definitions for compatibility */
#ifndef HAVE_UINT32
typedef uint32_t uint32;
#endif
#ifndef HAVE_UINT64
typedef uint64_t uint64;
#endif

/* Forward declarations */
typedef struct PgShmemConn PgShmemConn;
typedef struct PGShmemHeader PgShmemHeader;

/* Error codes */
typedef enum PgShmemError
{
	PGSHMEM_OK = 0,					/* Success */
	PGSHMEM_ERROR_INVALID_PID,		/* Invalid or non-existent PID */
	PGSHMEM_ERROR_NOT_POSTGRES,		/* Process is not PostgreSQL */
	PGSHMEM_ERROR_ACCESS_DENIED,	/* Permission denied */
	PGSHMEM_ERROR_ATTACH_FAILED,	/* Failed to attach to shared memory */
	PGSHMEM_ERROR_INVALID_SEGMENT,	/* Invalid shared memory segment */
	PGSHMEM_ERROR_STRUCT_NOT_FOUND,	/* Requested structure not found */
	PGSHMEM_ERROR_VERSION_MISMATCH,	/* PostgreSQL version mismatch */
	PGSHMEM_ERROR_MAGIC_MISMATCH,	/* Shared memory magic number mismatch */
	PGSHMEM_ERROR_OUT_OF_MEMORY,	/* Out of memory */
	PGSHMEM_ERROR_UNKNOWN			/* Unknown error */
} PgShmemError;

/* Structure information */
typedef struct PgShmemStructInfo
{
	const char *name;				/* Structure name (e.g., "XLOG Ctl") */
	void	   *address;			/* Address in shared memory */
	size_t		size;				/* Size of the structure */
	size_t		allocated_size;		/* Actually allocated size */
} PgShmemStructInfo;

/* Connection information */
typedef struct PgShmemConnInfo
{
	pid_t		postmaster_pid;		/* PostgreSQL postmaster PID */
	size_t		segment_size;		/* Total shared memory segment size */
	unsigned long segment_id;		/* Platform-specific segment ID */
	void	   *segment_address;	/* Base address of shared memory */
	uint32		pg_version;			/* PostgreSQL version number */
	uint64		system_identifier;	/* Database system identifier */
} PgShmemConnInfo;

/*
 * Core API functions
 */

/* Connect to PostgreSQL shared memory by postmaster PID */
extern PgShmemConn *PgShmemConnectByPid(pid_t postmaster_pid, PgShmemError *error);

/* Disconnect from shared memory */
extern void PgShmemDisconnect(PgShmemConn *conn);

/* Get connection information */
extern PgShmemError PgShmemGetConnInfo(PgShmemConn *conn, PgShmemConnInfo *info);

/* Get error message for error code */
extern const char *PgShmemErrorMessage(PgShmemError error);

/*
 * Structure access functions
 */

/* Find a named structure in shared memory */
extern void *PgShmemFindStruct(PgShmemConn *conn, const char *name, PgShmemError *error);

/* Get information about a named structure */
extern PgShmemError PgShmemGetStructInfo(PgShmemConn *conn, const char *name, 
										  PgShmemStructInfo *info);

/* List all available structures */
extern PgShmemError PgShmemListStructs(PgShmemConn *conn, PgShmemStructInfo **structs, 
										int *count);

/* Free structure list */
extern void PgShmemFreeStructList(PgShmemStructInfo *structs, int count);

/*
 * Utility functions
 */

/* Check if a PID is a PostgreSQL postmaster */
extern bool PgShmemIsPostmasterPid(pid_t pid);

/* Get shared memory segment ID for a PostgreSQL process */
extern PgShmemError PgShmemGetSegmentId(pid_t postmaster_pid, unsigned long *segment_id);

/* Validate shared memory magic and version */
extern PgShmemError PgShmemValidateSegment(void *segment_addr, size_t segment_size);

/*
 * Convenience functions for common structures
 */

/* Get XLOG Control structure */
extern void *PgShmemGetXLogCtl(PgShmemConn *conn, PgShmemError *error);

/* Get Control File data */
extern void *PgShmemGetControlFile(PgShmemConn *conn, PgShmemError *error);

/* Get Process Array */
extern void *PgShmemGetProcArray(PgShmemConn *conn, PgShmemError *error);

/*
 * Safety and validation functions
 */

/* Verify connection is still valid */
extern bool PgShmemConnectionValid(PgShmemConn *conn);

/* Check if PostgreSQL is still running */
extern bool PgShmemPostmasterAlive(PgShmemConn *conn);

/* Get safe copy of data (allocates memory) */
extern void *PgShmemGetStructCopy(PgShmemConn *conn, const char *name, 
								  size_t *size, PgShmemError *error);

/* Free memory allocated by PgShmemGetStructCopy */
extern void PgShmemFreeCopy(void *copy);

/*
 * Advanced functions for direct access (use with extreme caution)
 */

/* Get raw shared memory address */
extern void *PgShmemGetRawAddress(PgShmemConn *conn, size_t offset);

/* Get shared memory header */
extern PgShmemHeader *PgShmemGetHeader(PgShmemConn *conn);

/*
 * Version compatibility
 */
#define LIBPG_SHMEM_VERSION_MAJOR	1
#define LIBPG_SHMEM_VERSION_MINOR	0
#define LIBPG_SHMEM_VERSION_PATCH	0

/* Get library version */
extern void PgShmemGetLibraryVersion(int *major, int *minor, int *patch);

/* Check if library is compatible with PostgreSQL version */
extern bool PgShmemCheckCompatibility(uint32 pg_version);

/*
 * Constants
 */
#define PGSHMEM_MAGIC				679834894	/* Same as PGShmemMagic */
#define PGSHMEM_MAX_STRUCT_NAME		48			/* Maximum structure name length */

#ifdef __cplusplus
}
#endif

#endif							/* LIBPG_SHMEM_FE_H */ 