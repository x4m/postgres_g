/*-------------------------------------------------------------------------
 *
 * pg_index_partitions.h
 *	  definition of the system catalog (pg_index_partitions)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_index_partitions.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_INDEX_PARTITIONS_H
#define PG_INDEX_PARTITIONS_H

#include "catalog/genbki.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_index_partitions_d.h"
#include "catalog/pg_type_d.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"

/* ----------------
 *		pg_index_partitions definition.  cpp turns this into
 *		typedef struct FormData_pg_index_partitions
 * ----------------
 */
CATALOG(pg_index_partitions,6015,IndexPartitionsRelationId)
{
	Oid			indexoid BKI_LOOKUP(pg_class);
	Oid			reloid BKI_LOOKUP(pg_class);
	int32		partid;
} FormData_pg_index_partitions;

/* ----------------
 *		Form_pg_index_partitions corresponds to a pointer to a tuple with
 *		the format of pg_index_partitions relation.
 * ----------------
 */
typedef FormData_pg_index_partitions *Form_pg_index_partitions;

DECLARE_UNIQUE_INDEX_PKEY(pg_index_partitions_indexoid_partid_index, 6018, IndexPartitionsIndexId, pg_index_partitions, btree(indexoid oid_ops, partid int4_ops));

/*
 * Map over the pg_index_partitions table for a particular global index.  This
 * will be used for faster lookup of the next partid to be used for this global
 * index and also for finding out the partition relation for a give partid of
 * a global index.
 */
typedef struct IndexPartitionInfoData
{
	MemoryContext	context;	/* memory context for storing the cache data */
	PartitionId		max_partid;	/* max value of used partid */
	HTAB		   *pdir_hash;	/* partid to reloid lookup hash */
} IndexPartitionInfoData;

typedef IndexPartitionInfoData *IndexPartitionInfo;

/*
 * TODO we might think of storing the RelationDesc along with reloid?
 */
typedef struct IndexPartitionInfoEntry
{
	PartitionId	partid;		/* key */
	Oid			reloid;		/* payload */
} IndexPartitionInfoEntry;

#define		InvalidPartitionId			0
#define		FirstValidPartitionId		1
#define		PartIdIsValid(partid)	((bool) ((partid) != InvalidPartitionId))

extern void BuildIndexPartitionInfo(Relation relation, MemoryContext context);
extern PartitionId IndexGetRelationPartitionId(Relation irel, Oid reloid);
extern Oid IndexGetPartitionReloid(Relation irel, PartitionId partid);
extern PartitionId IndexGetNextPartitionID(Relation irel);
extern void DeleteIndexPartitionEntries(Oid indrelid);
extern void InsertIndexPartitionEntry(Relation irel, Oid reloid, PartitionId partid);
extern void InvalidateIndexPartitionEntries(List *reloids, Oid indexoid);
#endif							/* PG_INDEX_PARTITIONS_H */
