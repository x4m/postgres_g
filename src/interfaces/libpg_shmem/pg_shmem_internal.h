/*-------------------------------------------------------------------------
 *
 * pg_shmem_internal.h
 *	  PostgreSQL Shared Memory Access Library - Internal Definitions
 *
 * This header contains internal structures and function declarations
 * used within the libpg_shmem library.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/pg_shmem_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHMEM_INTERNAL_H
#define PG_SHMEM_INTERNAL_H

#include "libpg_shmem-fe.h"
#include "postgres_fe.h"

/* 
 * We need some PostgreSQL internal definitions, but we must be careful
 * about version compatibility. These are copied from the backend headers.
 */

#ifndef MAXPGPATH
#define MAXPGPATH 1024
#endif

/* From storage/pg_shmem.h */
#ifndef PGShmemMagic
#define PGShmemMagic 679834894
#endif

/* Forward declaration of backend structures we'll access */
typedef struct PGShmemHeader PGShmemHeader;
typedef struct ShmemIndexEnt ShmemIndexEnt;

/* Connection structure - must be defined here for use across source files */
struct PgShmemConn
{
	pid_t		postmaster_pid;		/* PostgreSQL postmaster PID */
	unsigned long segment_id;		/* Platform-specific segment ID */
	void	   *segment_address;	/* Base address of shared memory */
	size_t		segment_size;		/* Total segment size */
	PGShmemHeader *header;			/* Shared memory header */
	void	   *shmem_index;		/* Pointer to shmem index hash table */
	bool		attached;			/* True if successfully attached */
	char		datadir[MAXPGPATH]; /* PostgreSQL data directory */
};

/* Simplified definitions of PostgreSQL internal structures */
/* Note: We avoid redefining if already defined to prevent conflicts */
#ifndef PGSHMEM_HEADER_DEFINED
#define PGSHMEM_HEADER_DEFINED
struct PGShmemHeader
{
	int32		magic;			/* magic # to identify Postgres segments */
	pid_t		creatorPID;		/* PID of creating process */
	size_t		totalsize;		/* total size of segment */
	size_t		freeoffset;		/* offset to first free space */
	uint32		dsm_control;	/* ID of dynamic shared memory control seg */
	void	   *index;			/* pointer to ShmemIndex table */
#ifndef WIN32
	dev_t		device;			/* device data directory is on */
	ino_t		inode;			/* inode number of data directory */
#endif
};
#endif /* PGSHMEM_HEADER_DEFINED */

/* From storage/shmem.h */
#ifndef SHMEM_INDEX_KEYSIZE_DEFINED
#define SHMEM_INDEX_KEYSIZE_DEFINED
#define PGSHMEM_INDEX_KEYSIZE 48
#endif

#ifndef SHMEM_INDEX_ENT_DEFINED
#define SHMEM_INDEX_ENT_DEFINED
struct ShmemIndexEnt
{
	char		key[PGSHMEM_INDEX_KEYSIZE];	/* string name */
	void	   *location;		/* location in shared mem */
	size_t		size;			/* # bytes requested for the structure */
	size_t		allocated_size; /* # bytes actually allocated */
};
#endif /* SHMEM_INDEX_ENT_DEFINED */

/* Hash table structure (simplified) - we don't need the full definition */
typedef struct HTAB HTAB;
typedef struct HASHHDR HASHHDR;

/* Internal function declarations */

/* Utility functions */
extern bool pg_shmem_find_datadir_by_pid(pid_t pid, char *datadir);
extern bool pg_shmem_parse_cmdline(pid_t pid, char *datadir);
extern bool pg_shmem_check_postgres_process(pid_t pid);

/* Structure access functions */
extern void *pg_shmem_hash_search(HTAB *hashp, const void *keyPtr, bool *found);
extern HTAB *pg_shmem_get_index_table(PgShmemConn *conn);
extern ShmemIndexEnt *pg_shmem_find_index_entry(PgShmemConn *conn, const char *name);

/* Platform-specific functions */
#ifdef WIN32
extern PgShmemError pg_shmem_attach_win32(unsigned long segment_id, void **address, 
										   size_t *size);
extern PgShmemError pg_shmem_detach_win32(void *address, size_t size);
#else
extern PgShmemError pg_shmem_attach_posix(unsigned long segment_id, void **address, 
										   size_t *size);
extern PgShmemError pg_shmem_detach_posix(void *address, size_t size);
extern PgShmemError pg_shmem_get_segment_info_posix(unsigned long segment_id, size_t *size,
													 pid_t *creator_pid, int *nattach);
extern PgShmemError pg_shmem_list_segments_posix(int **segment_ids, int *count);
extern bool pg_shmem_check_sysv_support(void);
extern size_t pg_shmem_get_max_segment_size(void);
extern size_t pg_shmem_get_page_size(void);
#endif

/* Error handling */
extern void pg_shmem_set_error_context(const char *context);
extern const char *pg_shmem_get_error_context(void);

/* Memory validation */
extern bool pg_shmem_validate_pointer(PgShmemConn *conn, const void *ptr, size_t size);
extern bool pg_shmem_addr_in_segment(PgShmemConn *conn, const void *addr);

/* Connection state management */
extern bool pg_shmem_conn_is_valid(PgShmemConn *conn);
extern void pg_shmem_invalidate_conn(PgShmemConn *conn);

/* Structure-specific access helpers */
extern void *pg_shmem_get_xlog_ctl_internal(PgShmemConn *conn);
extern void *pg_shmem_get_control_file_internal(PgShmemConn *conn);
extern void *pg_shmem_get_proc_array_internal(PgShmemConn *conn);

/* Debug and logging functions */
#ifdef PG_SHMEM_DEBUG
extern void pg_shmem_debug_log(const char *fmt, ...) pg_attribute_printf(1, 2);
#define SHMEM_DEBUG_LOG(args) pg_shmem_debug_log args
#else
#define SHMEM_DEBUG_LOG(args) ((void) 0)
#endif

/* Constants */
#define PG_SHMEM_MAX_CONNECTIONS	64		/* Maximum concurrent connections */
#define PG_SHMEM_CONN_TIMEOUT		30		/* Connection timeout in seconds */
#define PG_SHMEM_MAX_RETRIES		3		/* Maximum retry attempts */

/* Common structure names in PostgreSQL */
#define XLOG_CTL_STRUCT_NAME		"XLOG Ctl"
#define CONTROL_FILE_STRUCT_NAME	"Control File"
#define PROC_ARRAY_STRUCT_NAME		"Proc Array"
#define SHMEM_INDEX_STRUCT_NAME		"ShmemIndex"
#define BUFFER_LOOKUP_STRUCT_NAME	"Buffer Lookup Table"
#define LOCK_MANAGER_STRUCT_NAME	"Shared LockMgr"

/* Macros for safe memory access */
/* Note: These are simplified versions, not the full PostgreSQL memory barriers */
#define PG_SHMEM_READ_BARRIER()		do { } while (0)
#define PG_SHMEM_WRITE_BARRIER()	do { } while (0)

/* Compatibility layer for different PostgreSQL versions */
typedef struct PgShmemVersionInfo
{
	uint32		version_num;		/* PostgreSQL version number */
	const char *version_string;		/* Version string */
	bool		compatible;			/* Whether this version is supported */
	size_t		xlog_ctl_size;		/* Size of XLogCtlData structure */
	size_t		control_file_size;	/* Size of ControlFileData structure */
} PgShmemVersionInfo;

extern const PgShmemVersionInfo *pg_shmem_get_version_info(uint32 version_num);
extern bool pg_shmem_version_is_compatible(uint32 version_num);

/* Thread safety (if needed) */
#ifdef ENABLE_THREAD_SAFETY
#include <pthread.h>
extern pthread_mutex_t pg_shmem_global_lock;
#define PG_SHMEM_LOCK()		pthread_mutex_lock(&pg_shmem_global_lock)
#define PG_SHMEM_UNLOCK()	pthread_mutex_unlock(&pg_shmem_global_lock)
#else
#define PG_SHMEM_LOCK()		((void) 0)
#define PG_SHMEM_UNLOCK()	((void) 0)
#endif

/* Connection registry for tracking active connections */
typedef struct PgShmemConnRegistry
{
	PgShmemConn *connections[PG_SHMEM_MAX_CONNECTIONS];
	int			count;
	bool		initialized;
} PgShmemConnRegistry;

extern PgShmemConnRegistry *pg_shmem_get_conn_registry(void);
extern bool pg_shmem_register_conn(PgShmemConn *conn);
extern void pg_shmem_unregister_conn(PgShmemConn *conn);
extern void pg_shmem_cleanup_connections(void);

#endif							/* PG_SHMEM_INTERNAL_H */ 