/*-------------------------------------------------------------------------
 *
 * compress_lz4.c
 *		lz4 compression method.
 *
 * Portions Copyright (c) 2016-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1990-1993, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compress_lz4.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres.h"
#include "access/compressamapi.h"
#include "access/toast_internals.h"
#include "fmgr.h"
#include "utils/builtins.h"

#ifdef HAVE_LIBLZ4
#include "lz4.h"

/*
 * lz4_cmcompress - compression routine for lz4 compression method
 *
 * Compresses source into dest using the default strategy. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
lz4_cmcompress(const struct varlena *value, int32 header_size)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	max_size = LZ4_compressBound(VARSIZE_ANY_EXHDR(value));
	tmp = (struct varlena *) palloc(max_size + header_size);

	len = LZ4_compress_default(VARDATA_ANY(value),
							   (char *) tmp + header_size,
							   valsize, max_size);
	if (len <= 0)
	{
		pfree(tmp);
		elog(ERROR, "lz4: could not compress data");
	}

	SET_VARSIZE_COMPRESSED(tmp, len + header_size);

	return tmp;
#endif
}

/*
 * lz4_cmdecompress - decompression routine for lz4 compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress(const struct varlena *value, int32 header_size)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		rawsize;
	struct varlena *result;

	result = (struct varlena *) palloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe((char *) value + header_size,
								  VARDATA(result),
								  VARSIZE(value) - header_size,
								  TOAST_COMPRESS_RAWSIZE(value));
	if (rawsize < 0)
		elog(ERROR, "lz4: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * lz4_cmdecompress_slice - slice decompression routine for lz4 compression
 *
 * Decompresses part of the data. Returns the decompressed varlena.
 */
static struct varlena *
lz4_cmdecompress_slice(const struct varlena *value, int32 header_size,
					  int32 slicelength)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	int32		rawsize;
	struct varlena *result;

	result = (struct varlena *) palloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe_partial((char *) value + header_size,
										  VARDATA(result),
										  VARSIZE(value) - header_size,
										  slicelength,
										  TOAST_COMPRESS_RAWSIZE(value));
	if (rawsize < 0)
		elog(ERROR, "lz4: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}
#endif

const CompressionAmRoutine lz4_compress_methods = {
	.type = T_CompressionAmRoutine,
	.datum_compress = lz4_cmcompress,
	.datum_decompress = lz4_cmdecompress,
	.datum_decompress_slice = lz4_cmdecompress_slice
};

/* lz4 compression handler function */
Datum
lz4handler(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBLZ4
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with lz4 support")));
#else
	PG_RETURN_POINTER(&lz4_compress_methods);
#endif
}
