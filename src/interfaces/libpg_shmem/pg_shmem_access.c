/*-------------------------------------------------------------------------
 *
 * pg_shmem_access.c
 *	  PostgreSQL Shared Memory Structure Access Functions
 *
 * This file contains functions for finding and accessing specific structures
 * within PostgreSQL's shared memory segment.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/interfaces/libpg_shmem/pg_shmem_access.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <string.h>
#include <errno.h>

#include "libpg_shmem-fe.h"
#include "pg_shmem_internal.h"

/* Global error context for better error reporting */
static char error_context[256] = "";

/* Error message table */
static const char *error_messages[] = {
	[PGSHMEM_OK] = "Success",
	[PGSHMEM_ERROR_INVALID_PID] = "Invalid or non-existent process ID",
	[PGSHMEM_ERROR_NOT_POSTGRES] = "Process is not a PostgreSQL server",
	[PGSHMEM_ERROR_ACCESS_DENIED] = "Permission denied accessing shared memory",
	[PGSHMEM_ERROR_ATTACH_FAILED] = "Failed to attach to shared memory segment",
	[PGSHMEM_ERROR_INVALID_SEGMENT] = "Invalid shared memory segment",
	[PGSHMEM_ERROR_STRUCT_NOT_FOUND] = "Requested structure not found in shared memory",
	[PGSHMEM_ERROR_VERSION_MISMATCH] = "PostgreSQL version mismatch",
	[PGSHMEM_ERROR_MAGIC_MISMATCH] = "Shared memory magic number mismatch",
	[PGSHMEM_ERROR_OUT_OF_MEMORY] = "Out of memory",
	[PGSHMEM_ERROR_UNKNOWN] = "Unknown error"
};

/* Internal hash table traversal state */
typedef struct HashTraverseState
{
	char	   *buffer;			/* Buffer for hash table data */
	size_t		buffer_size;	/* Size of buffer */
	int			num_entries;	/* Number of entries found */
	int			max_entries;	/* Maximum entries that fit in buffer */
} HashTraverseState;

/* Internal function declarations */
static ShmemIndexEnt *find_shmem_index_entry(PgShmemConn *conn, const char *name);
static bool traverse_hash_table(PgShmemConn *conn, PgShmemStructInfo **structs, int *count);
static bool validate_structure_access(PgShmemConn *conn, const void *ptr, size_t size);

/*
 * PgShmemErrorMessage
 *
 * Get a human-readable error message for an error code.
 */
const char *
PgShmemErrorMessage(PgShmemError error)
{
	if (error >= 0 && error < sizeof(error_messages) / sizeof(error_messages[0]))
	{
		const char *msg = error_messages[error];
		if (msg)
		{
			if (error_context[0])
			{
				static char full_message[512];
				snprintf(full_message, sizeof(full_message), "%s: %s", msg, error_context);
				return full_message;
			}
			return msg;
		}
	}
	return "Unknown error code";
}

/*
 * PgShmemFindStruct
 *
 * Find a named structure in PostgreSQL shared memory.
 */
void *
PgShmemFindStruct(PgShmemConn *conn, const char *name, PgShmemError *error)
{
	ShmemIndexEnt *entry;
	
	if (error)
		*error = PGSHMEM_OK;

	/* Validate inputs */
	if (!conn || !conn->attached || !name)
	{
		if (error)
			*error = PGSHMEM_ERROR_INVALID_SEGMENT;
		pg_shmem_set_error_context("Invalid connection or structure name");
		return NULL;
	}

	/* Check connection validity */
	if (!PgShmemConnectionValid(conn))
	{
		if (error)
			*error = PGSHMEM_ERROR_INVALID_SEGMENT;
		pg_shmem_set_error_context("Connection is no longer valid");
		return NULL;
	}

	/* Find the structure in the shared memory index */
	entry = find_shmem_index_entry(conn, name);
	if (!entry)
	{
		if (error)
			*error = PGSHMEM_ERROR_STRUCT_NOT_FOUND;
		pg_shmem_set_error_context(name);
		return NULL;
	}

	/* Validate the structure location */
	if (!validate_structure_access(conn, entry->location, entry->size))
	{
		if (error)
			*error = PGSHMEM_ERROR_INVALID_SEGMENT;
		pg_shmem_set_error_context("Structure location validation failed");
		return NULL;
	}

	SHMEM_DEBUG_LOG(("Found structure '%s' at %p, size %zu", 
					  name, entry->location, entry->size));

	return entry->location;
}

/*
 * PgShmemGetStructInfo
 *
 * Get information about a named structure.
 */
PgShmemError
PgShmemGetStructInfo(PgShmemConn *conn, const char *name, PgShmemStructInfo *info)
{
	ShmemIndexEnt *entry;

	/* Validate inputs */
	if (!conn || !conn->attached || !name || !info)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	/* Check connection validity */
	if (!PgShmemConnectionValid(conn))
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	/* Find the structure in the shared memory index */
	entry = find_shmem_index_entry(conn, name);
	if (!entry)
		return PGSHMEM_ERROR_STRUCT_NOT_FOUND;

	/* Fill in the structure information */
	memset(info, 0, sizeof(PgShmemStructInfo));
	info->name = name;  /* Note: this points to the caller's string */
	info->address = entry->location;
	info->size = entry->size;
	info->allocated_size = entry->allocated_size;

	return PGSHMEM_OK;
}

/*
 * PgShmemListStructs
 *
 * Get a list of all structures in shared memory.
 */
PgShmemError
PgShmemListStructs(PgShmemConn *conn, PgShmemStructInfo **structs, int *count)
{
	/* Validate inputs */
	if (!conn || !conn->attached || !structs || !count)
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	/* Check connection validity */
	if (!PgShmemConnectionValid(conn))
		return PGSHMEM_ERROR_INVALID_SEGMENT;

	/* Initialize outputs */
	*structs = NULL;
	*count = 0;

	/* Traverse the hash table to build the list */
	if (!traverse_hash_table(conn, structs, count))
		return PGSHMEM_ERROR_UNKNOWN;

	return PGSHMEM_OK;
}

/*
 * PgShmemFreeStructList
 *
 * Free a structure list returned by PgShmemListStructs.
 */
void
PgShmemFreeStructList(PgShmemStructInfo *structs, int count)
{
	if (structs)
		free(structs);
}

/*
 * PgShmemGetStructCopy
 *
 * Get a safe copy of a structure (allocates memory).
 */
void *
PgShmemGetStructCopy(PgShmemConn *conn, const char *name, size_t *size, PgShmemError *error)
{
	void	   *struct_ptr;
	void	   *copy;
	ShmemIndexEnt *entry;

	if (error)
		*error = PGSHMEM_OK;
	if (size)
		*size = 0;

	/* Find the structure */
	struct_ptr = PgShmemFindStruct(conn, name, error);
	if (!struct_ptr)
		return NULL;

	/* Get structure information to determine size */
	entry = find_shmem_index_entry(conn, name);
	if (!entry)
	{
		if (error)
			*error = PGSHMEM_ERROR_STRUCT_NOT_FOUND;
		return NULL;
	}

	/* Allocate memory for the copy */
	copy = malloc(entry->size);
	if (!copy)
	{
		if (error)
			*error = PGSHMEM_ERROR_OUT_OF_MEMORY;
		return NULL;
	}

	/* Copy the data */
	memcpy(copy, struct_ptr, entry->size);

	if (size)
		*size = entry->size;

	return copy;
}

/*
 * PgShmemFreeCopy
 *
 * Free memory allocated by PgShmemGetStructCopy.
 */
void
PgShmemFreeCopy(void *copy)
{
	if (copy)
		free(copy);
}

/*
 * Convenience functions for common structures
 */

/*
 * PgShmemGetXLogCtl
 *
 * Get the XLOG Control structure.
 */
void *
PgShmemGetXLogCtl(PgShmemConn *conn, PgShmemError *error)
{
	return PgShmemFindStruct(conn, XLOG_CTL_STRUCT_NAME, error);
}

/*
 * PgShmemGetControlFile
 *
 * Get the Control File structure.
 */
void *
PgShmemGetControlFile(PgShmemConn *conn, PgShmemError *error)
{
	return PgShmemFindStruct(conn, CONTROL_FILE_STRUCT_NAME, error);
}

/*
 * PgShmemGetProcArray
 *
 * Get the Process Array structure.
 */
void *
PgShmemGetProcArray(PgShmemConn *conn, PgShmemError *error)
{
	return PgShmemFindStruct(conn, PROC_ARRAY_STRUCT_NAME, error);
}

/*
 * Advanced access functions
 */

/*
 * PgShmemGetRawAddress
 *
 * Get a raw address in shared memory (use with extreme caution).
 */
void *
PgShmemGetRawAddress(PgShmemConn *conn, size_t offset)
{
	if (!conn || !conn->attached)
		return NULL;

	if (offset >= conn->segment_size)
		return NULL;

	return (char *) conn->segment_address + offset;
}

/*
 * PgShmemGetHeader
 *
 * Get the shared memory header.
 */
PgShmemHeader *
PgShmemGetHeader(PgShmemConn *conn)
{
	if (!conn || !conn->attached)
		return NULL;

	return conn->header;
}

/*
 * Version and compatibility functions
 */

/*
 * PgShmemGetLibraryVersion
 *
 * Get the library version.
 */
void
PgShmemGetLibraryVersion(int *major, int *minor, int *patch)
{
	if (major)
		*major = LIBPG_SHMEM_VERSION_MAJOR;
	if (minor)
		*minor = LIBPG_SHMEM_VERSION_MINOR;
	if (patch)
		*patch = LIBPG_SHMEM_VERSION_PATCH;
}

/*
 * PgShmemCheckCompatibility
 *
 * Check if the library is compatible with a PostgreSQL version.
 */
bool
PgShmemCheckCompatibility(uint32 pg_version)
{
	/* For now, we support PostgreSQL 17+ */
	return (pg_version >= 170000);
}

/*
 * Internal helper functions
 */

/*
 * find_shmem_index_entry
 *
 * Find an entry in the shared memory index hash table.
 * This is a simplified implementation that doesn't use the full hash table API.
 */
static ShmemIndexEnt *
find_shmem_index_entry(PgShmemConn *conn, const char *name)
{
	/* This is a simplified implementation. In a full implementation,
	 * we would need to properly traverse the PostgreSQL hash table structure.
	 * For now, we'll implement a basic linear search through the index.
	 */
	
	/* TODO: Implement proper hash table traversal */
	/* This would require understanding the PostgreSQL HTAB structure layout */
	
	SHMEM_DEBUG_LOG(("Searching for structure: %s", name));
	
	/* Placeholder implementation */
	pg_shmem_set_error_context("Hash table traversal not yet implemented");
	return NULL;
}

/*
 * traverse_hash_table
 *
 * Traverse the shared memory index to build a list of all structures.
 */
static bool
traverse_hash_table(PgShmemConn *conn, PgShmemStructInfo **structs, int *count)
{
	/* TODO: Implement hash table traversal */
	pg_shmem_set_error_context("Hash table enumeration not yet implemented");
	return false;
}

/*
 * validate_structure_access
 *
 * Validate that a structure can be safely accessed.
 */
static bool
validate_structure_access(PgShmemConn *conn, const void *ptr, size_t size)
{
	/* Check that the pointer is within the shared memory segment */
	if (!pg_shmem_addr_in_segment(conn, ptr))
		return false;

	/* Check that the entire structure is within the segment */
	if ((char *) ptr + size > (char *) conn->segment_address + conn->segment_size)
		return false;

	return true;
}

/*
 * Error context management
 */
void
pg_shmem_set_error_context(const char *context)
{
	if (context)
	{
		strncpy(error_context, context, sizeof(error_context) - 1);
		error_context[sizeof(error_context) - 1] = '\0';
	}
	else
	{
		error_context[0] = '\0';
	}
}

const char *
pg_shmem_get_error_context(void)
{
	return error_context[0] ? error_context : NULL;
} 