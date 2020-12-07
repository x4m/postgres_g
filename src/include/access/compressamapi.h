/*-------------------------------------------------------------------------
 *
 * compressamapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * src/include/access/compressamapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSAMAPI_H
#define COMPRESSAMAPI_H

#include "postgres.h"

#include "catalog/pg_am_d.h"
#include "nodes/nodes.h"

/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression method oid.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	LZ4_COMPRESSION_ID = 1,
	/* one free slot for the future built-in method */
	CUSTOM_COMPRESSION_ID = 3
} CompressionId;

/* Use default compression method if it is not specified. */
#define DefaultCompressionOid	PGLZ_COMPRESSION_AM_OID
#define IsCustomCompression(cmid)     ((cmid) == CUSTOM_COMPRESSION_ID)
#define IsStorageCompressible(storage) ((storage) != TYPSTORAGE_PLAIN && \
										(storage) != TYPSTORAGE_EXTERNAL)
/* compression handler routines */
typedef struct varlena *(*cmcompress_function) (const struct varlena *value,
												int32 toast_header_size);
typedef struct varlena *(*cmdecompress_function) (const struct varlena *value,
												  int32 toast_header_size);
typedef struct varlena *(*cmdecompress_slice_function)
												(const struct varlena *value,
												 int32 toast_header_size,
												 int32 slicelength);

/*
 * API struct for a compression AM.
 *
 * 'datum_compress' - varlena compression function.
 * 'datum_decompress' - varlena decompression function.
 * 'datum_decompress_slice' - varlena slice decompression functions.
 */
typedef struct CompressionAmRoutine
{
	NodeTag		type;

	cmcompress_function datum_compress;
	cmdecompress_function datum_decompress;
	cmdecompress_slice_function datum_decompress_slice;
} CompressionAmRoutine;

extern const CompressionAmRoutine pglz_compress_methods;
extern const CompressionAmRoutine lz4_compress_methods;

/* access/compression/compressamapi.c */
extern CompressionId CompressionOidToId(Oid cmoid);
extern Oid CompressionIdToOid(CompressionId cmid);
extern CompressionAmRoutine *GetCompressionAmRoutineByAmId(Oid amoid);

#endif							/* COMPRESSAMAPI_H */
