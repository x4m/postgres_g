/*-------------------------------------------------------------------------
 *
 * compress_pglz.c
 *	  pglz compression method
 *
 * Copyright (c) 2015-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compress_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/compressamapi.h"
#include "access/toast_internals.h"
#include "common/pg_lzcompress.h"

#include "fmgr.h"
#include "utils/builtins.h"

/*
 * pglz_cmcompress - compression routine for pglz compression method
 *
 * Compresses source into dest using the default strategy. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
pglz_cmcompress(const struct varlena *value)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	/*
	 * No point in wasting a palloc cycle if value size is out of the allowed
	 * range for compression
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return NULL;

	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									TOAST_COMPRESS_HDRSZ);

	len = pglz_compress(VARDATA_ANY(DatumGetPointer(value)),
						valsize,
						TOAST_COMPRESS_RAWDATA(tmp),
						NULL);

	if (len >= 0)
	{
		SET_VARSIZE_COMPRESSED(tmp, len + TOAST_COMPRESS_HDRSZ);
		return tmp;
	}

	pfree(tmp);

	return NULL;
}

/*
 * pglz_cmdecompress - decompression routine for pglz compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress(const struct varlena *value)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = pglz_decompress(TOAST_COMPRESS_RAWDATA(value),
							  TOAST_COMPRESS_SIZE(value),
							  VARDATA(result),
							  TOAST_COMPRESS_RAWSIZE(value), true);

	if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * pglz_decompress - slice decompression routine for pglz compression method
 *
 * Decompresses part of the data. Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress_slice(const struct varlena *value,
						int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	rawsize = pglz_decompress(TOAST_COMPRESS_RAWDATA(value),
							  VARSIZE(value) - TOAST_COMPRESS_HDRSZ,
							  VARDATA(result),
							  slicelength, false);

	if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

const CompressionAmRoutine pglz_compress_methods = {
	.type = T_CompressionAmRoutine,
	.datum_compress = pglz_cmcompress,
	.datum_decompress = pglz_cmdecompress,
	.datum_decompress_slice = pglz_cmdecompress_slice
};

/* pglz compression handler function */
Datum
pglzhandler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&pglz_compress_methods);
}
