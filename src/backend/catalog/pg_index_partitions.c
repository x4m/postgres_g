/*-------------------------------------------------------------------------
 *
 * pg_index_partitions.c
 *
 *	  routines to support manipulation of the pg_index_partitions relation and
 *	  also provide cache over this relation for faster mapping from partition
 *	  id to reloid for a global index.
 *
 * Notes(why we need pg_index_partitions relation):
 *
 * This mapping is required for global indexes.  Basically, global indexes
 * stores tuple from multiple partitions of a partitioned relation so TIDs
 * alone are insufficient for uniquely identifying tuples. We might consider
 * storing the relation OID along with TID, as the combination of relation OID
 * and TID is the most straightforward way to uniquely identify a heap tuple.
 *
 * However, this approach has its drawbacks. For instance, if a partition is
 * detached, the global index would not know how to ignore the tuples from that
 * detached partition unless it cleans out all tuples from that partition in
 * the global index.
 *
 * Therefore, we need an identifier for each partition that is only valid while
 * the partition is attached and becomes invalid once the partition is
 * detached. We call this identifier a partition ID. However, when accessing
 * tuples from the index, we still need to convert the partition ID to the heap
 * OID at some point.  One might assume a 1-to-1 mapping between partition IDs
 * and relation OIDs, but this isn't feasible. In a multi-level partition
 * hierarchy, detaching a partitioned table from a higher-level partitioned
 * table invalidates the underlying leaf relation's partition ID for the global
 * indexes of the parent from which it was detached. However, that partition
 * ID should remain valid for the global indexes prersent at the lower level
 * partitioned tables where the leaf partition is still attached. Therefore,
 * for simplicity, we maintain a partition ID for each global index and leaf
 * relation OID pair. This way, detaching a partition only requires
 * invalidating the specific global index and partition ID combinations from
 * which the leaf partition is being detached.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_index_partitions.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/catalog.h"
#include "catalog/pg_index_partitions.h"
#include "partitioning/partdesc.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/rel.h"

/*
 * InsertIndexPartitionEntry - Insert parition id to reloid mapping
 *
 * Insert (indexoid, partid) to reloid mapping into pg_index_partitions table.
 */
void
InsertIndexPartitionEntry(Relation irel, Oid reloid, PartitionId partid)
{
	Datum		values[Natts_pg_index_partitions];
	bool		nulls[Natts_pg_index_partitions];
	HeapTuple	tuple;
	Relation	rel;
	Oid			indexoid = RelationGetRelid(irel);

	rel = table_open(IndexPartitionsRelationId, RowExclusiveLock);

	/* Make the pg_index_partitions entry. */
	values[Anum_pg_index_partitions_indexoid - 1] = ObjectIdGetDatum(indexoid);
	values[Anum_pg_index_partitions_reloid - 1] = ObjectIdGetDatum(reloid);
	values[Anum_pg_index_partitions_partid - 1] = PartitionIdGetDatum(partid);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	table_close(rel, RowExclusiveLock);
}

/*
 * DeleteIndexPartitionEntries - Delete all index partition entries.
 *
 * This will delete all the entires for given global index id from
 * pg_index_partitions table.  This should only be called when global index
 * is being dropped.
 */
void
DeleteIndexPartitionEntries(Oid indrelid)
{
	Relation	catalogRelation;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;

	/* Find pg_index_partitions entries by indrelid. */
	catalogRelation = table_open(IndexPartitionsRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_index_partitions_indexoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(indrelid));

	scan = systable_beginscan(catalogRelation, IndexPartitionsIndexId, true,
							  NULL, 1, &key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		CatalogTupleDelete(catalogRelation, &tuple->t_self);

	/* Done */
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * BuildIndexPartitionInfo - Cache for parittion id to reloid mapping
 *
 * Build a cache for faster access to the mappping from partition id to the
 * relation oid.  For more detail on this mapping refer to the comments in
 * pg_index_partition.h and also atop PartitionId declaration in c.h.
 */
void
BuildIndexPartitionInfo(Relation relation, MemoryContext context)
{
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	Relation	rel;
	PartitionId	maxpartid = InvalidPartitionId;
	IndexPartitionInfo	map;
	MemoryContext oldcontext;
	HASHCTL		ctl;

	/*
	 * Open pg_index_partition table for getting the partition id to reloid
	 * mapping for the input index relation.
	 */
	rel = table_open(IndexPartitionsRelationId, AccessShareLock);

	oldcontext = MemoryContextSwitchTo(context);
	map = (IndexPartitionInfoData *) palloc0(sizeof(IndexPartitionInfoData));
	map->context = context;

	/* Make a new hash table for the cache */
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(IndexPartitionInfoEntry);
	ctl.hcxt = context;

	map->pdir_hash = hash_create("index partition directory", 256, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(oldcontext);

	ScanKeyInit(&key,
				Anum_pg_index_partitions_indexoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));

	scan = systable_beginscan(rel, IndexPartitionsIndexId, true,
							  NULL, 1, &key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);
		IndexPartitionInfoEntry *entry;
		bool		found;

		/*
		 * We need to consider the partition id of the detached partitioned as
		 * well while computing the maxpartid so that we do not repeat the
		 * value.
		 */
		if (form->partid > maxpartid)
			maxpartid = form->partid;

		if (!OidIsValid(form->reloid))
			continue;

		entry = hash_search(map->pdir_hash, &form->partid, HASH_ENTER, &found);
		Assert(!found);
		entry->reloid = form->reloid;
	}

	map->max_partid = maxpartid;
	relation->rd_indexpartinfo = map;
	systable_endscan(scan);

	table_close(rel, AccessShareLock);
}

/*
 * IndexGetRelationPartitionId - Get partition id for the reloid
 *
 * Get the partition ID for the given partition relation OID for the specified
 * global index relation.
 */
PartitionId
IndexGetRelationPartitionId(Relation irel, Oid reloid)
{
	IndexPartitionInfo	map;
	HASH_SEQ_STATUS		hash_seq;
	PartitionId			partid = InvalidPartitionId;
	IndexPartitionInfoEntry *entry;

	if (irel->rd_indexpartinfo == NULL)
		BuildIndexPartitionInfo(irel, CurrentMemoryContext);

	map = irel->rd_indexpartinfo;

	hash_seq_init(&hash_seq, map->pdir_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (entry->reloid == reloid)
		{
			partid = entry->partid;
			hash_seq_term(&hash_seq);
			break;
		}
	}

	return partid;
}

/*
 * IndexGetPartitionReloid - Get relation oid for the paritionid
 *
 * Get the relation OID for the given partition ID for the specified global
 * index relation.
 */
Oid
IndexGetPartitionReloid(Relation irel, PartitionId partid)
{
	IndexPartitionInfo	map = irel->rd_indexpartinfo;
	IndexPartitionInfoEntry *entry;
	bool		found;

	entry = hash_search(map->pdir_hash, &partid, HASH_FIND, &found);
	if (!found)
		return InvalidOid;

	return entry->reloid;
}

/*
 * InvalidateIndexPartitionEntries - Invalidate pg_index_partitions entries
 *
 * Set reloid as Invalid in pg_index_partitions entries with respect to the
 * given reloid.  If a valid reloids list is given then only
 * invalidate the reloid entires which are related to the input reloids.
 */
void
InvalidateIndexPartitionEntries(List *reloids, Oid indexoid)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;

	/*
	 * Find pg_inherits entries by inhparent.  (We need to scan them all in
	 * order to verify that no other partition is pending detach.)
	 */
	catalogRelation = table_open(IndexPartitionsRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_index_partitions_indexoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(indexoid));

	scan = systable_beginscan(catalogRelation, IndexPartitionsIndexId, true,
							  NULL, 1, &key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);
		HeapTuple	newtup;

		if (!list_member_oid(reloids, form->reloid))
			continue;

		newtup = heap_copytuple(tuple);
		((Form_pg_index_partitions) GETSTRUCT(newtup))->reloid = InvalidOid;

		CatalogTupleUpdate(catalogRelation,
						   &tuple->t_self,
						   newtup);
		heap_freetuple(newtup);
	}

	/* Done */
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);
}

/*
 * IndexGetNextPartitionID - Get the next partition ID of the global index
 *
 * Obtain the next partition ID to be allocated for the specified global index
 * relation. Also update this value in the cache for the next allocation.
 */
PartitionId
IndexGetNextPartitionID(Relation irel)
{
	PartitionId partid;

	/*
	 * If the cache is not already build then do it first so that we know what
	 * is the maximum partition ID value and then we can generate the next
	 * value.
	 */
	if (irel->rd_indexpartinfo == NULL)
		BuildIndexPartitionInfo(irel, CurrentMemoryContext);

	/* Use the max_partid + 1 value as the next parition id. */
	partid = irel->rd_indexpartinfo->max_partid + 1;

	/*
	 * If partitionID is wraparound then give error.
	 * XXX here we might consider reusing the unused partition IDs.
	 */
	if (!PartIdIsValid(partid))
		elog(ERROR, "could not allocate new PartitionID because limit is exhausted");

	/*
	 * Store the new value in the cache, in case the cache is invalidated we
	 * will get the max value again from the system catalog, so there should
	 * not be any issue.
	 */
	irel->rd_indexpartinfo->max_partid = partid;

	return partid;
}

/*
 * IndexPartitionRelidGlobalIndexList - Get global index list for give reloid
 *
 * Get list of all the global index for given relation oid.
 */
List *
IndexPartitionRelidGetGlobalIndexOids(Oid reloid)
{
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	List	   *globalindexoids = NIL;

	/*
	 * Find pg_inherits entries by inhparent.  (We need to scan them all in
	 * order to verify that no other partition is pending detach.)
	 */
	catalogRelation = table_open(IndexPartitionsRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_index_partitions_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(reloid));

	scan = systable_beginscan(catalogRelation, IndexPartitionsReloidIndexId,
							  true, NULL, 1, &key);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_index_partitions form = (Form_pg_index_partitions) GETSTRUCT(tuple);

		Assert(form->reloid == reloid);
		globalindexoids = lappend_oid(globalindexoids, form->indexoid);
	}

	/* Done */
	systable_endscan(scan);
	table_close(catalogRelation, RowExclusiveLock);

	return globalindexoids;
}
