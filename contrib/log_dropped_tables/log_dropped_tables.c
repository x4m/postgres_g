/*-------------------------------------------------------------------------
 *
 * log_dropped_tables.c
 *	  Log information about dropped tables at transaction commit
 *
 * This extension uses object access hooks to capture table names when
 * they are dropped, then logs them with their commit LSN at transaction end.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

/* Hash table to track dropped relations with their names */
typedef struct DroppedRelInfo
{
	Oid			reloid;				/* hash key */
	char		relname[NAMEDATALEN];
	char		schemaname[NAMEDATALEN];
	Oid			tablespace;
	Oid			database;
	RelFileNumber relfilenode;
} DroppedRelInfo;

static HTAB *dropped_rels_hash = NULL;
static object_access_hook_type prev_object_access_hook = NULL;

/* Module initialization */
void		_PG_init(void);
void		_PG_fini(void);

/* Object access hook to capture dropped tables */
static void log_dropped_object_access(ObjectAccessType access,
									  Oid classId,
									  Oid objectId,
									  int subId,
									  void *arg);

/*
 * Object access hook to capture dropped tables before catalog is cleaned
 */
static void
log_dropped_object_access(ObjectAccessType access,
						  Oid classId,
						  Oid objectId,
						  int subId,
						  void *arg)
{
	/* Call previous hook if exists */
	if (prev_object_access_hook)
		prev_object_access_hook(access, classId, objectId, subId, arg);

	/* We only care about drops of relations */
	if (access == OAT_DROP && classId == RelationRelationId)
	{
		HeapTuple	tuple;
		Form_pg_class classForm;
		DroppedRelInfo *entry;
		bool		found;

		/* Get relation info from catalog while it still exists */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objectId));
		if (!HeapTupleIsValid(tuple))
			return;

		classForm = (Form_pg_class) GETSTRUCT(tuple);

		/* Only track tables (not indexes, sequences, etc. unless you want) */
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_PARTITIONED_TABLE)
		{
			ReleaseSysCache(tuple);
			return;
		}

		/* Initialize hash table if needed */
		if (dropped_rels_hash == NULL)
		{
			HASHCTL		ctl;

			ctl.keysize = sizeof(Oid);
			ctl.entrysize = sizeof(DroppedRelInfo);
			ctl.hcxt = TopTransactionContext;
			dropped_rels_hash = hash_create("Dropped Relations Hash",
											32,
											&ctl,
											HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		}

		/* Store the relation info */
		entry = (DroppedRelInfo *) hash_search(dropped_rels_hash,
											   &objectId,
											   HASH_ENTER,
											   &found);

		if (!found)
		{
			char	   *schemaname = get_namespace_name(classForm->relnamespace);

			strlcpy(entry->relname, NameStr(classForm->relname), NAMEDATALEN);
			strlcpy(entry->schemaname, schemaname ? schemaname : "unknown", NAMEDATALEN);
			entry->tablespace = classForm->reltablespace;
			entry->database = MyDatabaseId;
			entry->relfilenode = classForm->relfilenode;

			if (schemaname)
				pfree(schemaname);
		}

		ReleaseSysCache(tuple);
	}
}

/*
 * Transaction callback to log dropped tables with their commit LSN
 */
static void
log_dropped_tables_callback(XactEvent event, XLogRecPtr lsn, void *arg)
{
	HASH_SEQ_STATUS status;
	DroppedRelInfo *entry;

	/* Only log on successful commit */
	if (event != XACT_EVENT_COMMIT)
		goto cleanup;

	/* Nothing to log if no tables were dropped */
	if (dropped_rels_hash == NULL)
		return;

	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			count = hash_get_num_entries(dropped_rels_hash);

		if (count > 0)
		{
			ereport(INFO,
					(errmsg("Transaction commit at LSN %X/%X dropping %d table(s)",
							LSN_FORMAT_ARGS(lsn), count)));

			/* Log each dropped table with its name */
			hash_seq_init(&status, dropped_rels_hash);
			while ((entry = (DroppedRelInfo *) hash_seq_search(&status)) != NULL)
			{
				ereport(INFO,
						(errmsg("Dropped table: %s.%s (OID %u, relfilenode %u) at LSN %X/%X",
								entry->schemaname,
								entry->relname,
								entry->reloid,
								entry->relfilenode,
								LSN_FORMAT_ARGS(lsn))));
			}
		}
	}
	else
	{
		/* LSN is invalid - log a warning for debugging */
		ereport(WARNING,
				(errmsg("Cannot log dropped tables: commit LSN is invalid (transaction may not have written WAL)")));
	}

cleanup:
	/* Clean up the hash table for the next transaction */
	if (dropped_rels_hash != NULL)
	{
		//hash_destroy(dropped_rels_hash);
		//dropped_rels_hash = NULL;
	}
}

/*
 * Module initialization function
 */
void
_PG_init(void)
{
	/* Install our object access hook */
	prev_object_access_hook = object_access_hook;
	object_access_hook = log_dropped_object_access;

	/* Register transaction callback */
	RegisterXactCallback(log_dropped_tables_callback, NULL);

	ereport(LOG,
			(errmsg("log_dropped_tables extension loaded")));
}

/*
 * Module cleanup function
 */
void
_PG_fini(void)
{
	/* Restore previous hook */
	object_access_hook = prev_object_access_hook;
}
