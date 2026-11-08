/*-------------------------------------------------------------------------
 *
 * import.c
 *    Experimental catalog reconstruction for physical imports.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/bin/pg_upgrade/import.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <dirent.h>

#include "common/file_perm.h"
#include "fe_utils/string_utils.h"
#include "pg_upgrade.h"

typedef struct ImportExtension
{
	char	   *database;
	char	   *name;
	char	   *version;
	char	   *schema;
	struct ImportExtension *next;
} ImportExtension;

typedef struct ImportDatabase
{
	char	   *name;
	Oid			oid;
	char	   *encoding;
	char		provider;
	char	   *collate;
	char	   *ctype;
	char	   *locale;
	char	   *icurules;
	char	   *collversion;
	uint32		frozen;
	uint32		minmulti;
	struct ImportDatabase *next;
} ImportDatabase;

static ImportExtension *extensions;
static ImportDatabase *databases;
static char *import_config;

static void
import_literal(PQExpBuffer buf, PGconn *conn, const char *value)
{
	char	   *literal = PQescapeLiteral(conn, value, strlen(value));

	if (!literal)
		pg_fatal("could not quote import metadata: %s", PQerrorMessage(conn));
	appendPQExpBufferStr(buf, literal);
	PQfreemem(literal);
}

/*
 * Use no source configuration, including authentication and ALTER SYSTEM
 * settings.  The source must already have completed recovery in isolation.
 * This mode does not make malformed native catalog/page contents safe.
 */
void
prepare_import(void)
{
	FILE	   *fp;
	char	   *hba = psprintf("%s/import_hba.conf", log_opts.dumpdir);
	char	   *twophase = psprintf("%s/pg_twophase", old_cluster.pgdata);
	DIR		   *dir;
	struct dirent *de;

	if ((dir = opendir(twophase)) == NULL)
		pg_fatal("could not open directory \"%s\": %m", twophase);
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0)
			pg_fatal("the import prototype requires an empty pg_twophase directory");
	}
	if (errno)
		pg_fatal("could not read directory \"%s\": %m", twophase);
	closedir(dir);
	pg_free(twophase);

	import_config = psprintf("%s/import.conf", log_opts.dumpdir);
	if ((fp = fopen(import_config, "w")) == NULL)
		pg_fatal("could not create file \"%s\": %m", import_config);
	if (fclose(fp) != 0)
		pg_fatal("could not close file \"%s\": %m", import_config);
	if ((fp = fopen(hba, "w")) == NULL)
		pg_fatal("could not create file \"%s\": %m", hba);
	fprintf(fp, "local all all trust\n");
	if (fclose(fp) != 0)
		pg_fatal("could not close file \"%s\": %m", hba);
	pg_free(hba);

	pg_log(PG_WARNING,
		   "Experimental physical import: keep the source and native data consumers isolated.\n"
		   "This prototype is not a security boundary or a data-integrity validator.");
}

void
import_server_options(PQExpBuffer options, ClusterInfo *cluster)
{
	if (!user_opts.import_mode)
		return;

	appendPQExpBufferStr(options, " -c binary_upgrade_import=on");
	if (cluster == &new_cluster)
		appendPQExpBufferStr(options,
							 " -c binary_upgrade_import_trusted_extensions=on");
	else
	{
		char	   *hba = psprintf("%s/import_hba.conf", log_opts.dumpdir);

		appendPQExpBufferStr(options, " -c config_file=");
		appendShellString(options, import_config);
		appendPQExpBufferStr(options, " -c hba_file=");
		appendShellString(options, hba);
		pg_free(hba);
	}
}

/* Fail closed for storage contracts the first prototype does not handle. */
void
check_import_source(void)
{
	PGconn	   *conn = connectToServer(&old_cluster, "template1");
	PGresult   *dbs;
	PGresult   *res;
	int			dbnum;

	prep_status("Checking experimental import contracts");
	dbs = executeQueryOrDie(conn,
							"SELECT d.datname, d.oid, pg_catalog.pg_encoding_to_char(d.encoding), "
							"d.datlocprovider, d.datcollate, d.datctype, d.datlocale, "
							"d.daticurules, d.datcollversion, d.datfrozenxid, d.datminmxid, d.dattablespace "
							"FROM pg_catalog.pg_database d WHERE " IMPORT_DB_FILTER " "
							"AND d.datallowconn AND d.datconnlimit <> -2 ORDER BY d.oid");
	PQfinish(conn);

	for (dbnum = 0; dbnum < PQntuples(dbs); dbnum++)
	{
		const char *dbname = PQgetvalue(dbs, dbnum, 0);
		ImportDatabase *db = pg_malloc0_object(ImportDatabase);
		int			i;

		db->name = pg_strdup(dbname);
		db->oid = atooid(PQgetvalue(dbs, dbnum, 1));
		if (db->oid < 16384 || atooid(PQgetvalue(dbs, dbnum, 11)) != 1663)
			pg_fatal("physical import requires normal database OIDs and the default tablespace");
		db->encoding = pg_strdup(PQgetvalue(dbs, dbnum, 2));
		db->provider = PQgetvalue(dbs, dbnum, 3)[0];
		db->collate = pg_strdup(PQgetvalue(dbs, dbnum, 4));
		db->ctype = pg_strdup(PQgetvalue(dbs, dbnum, 5));
		db->locale = PQgetisnull(dbs, dbnum, 6) ? NULL : pg_strdup(PQgetvalue(dbs, dbnum, 6));
		db->icurules = PQgetisnull(dbs, dbnum, 7) ? NULL : pg_strdup(PQgetvalue(dbs, dbnum, 7));
		db->collversion = PQgetisnull(dbs, dbnum, 8) ? NULL : pg_strdup(PQgetvalue(dbs, dbnum, 8));
		db->frozen = str2uint(PQgetvalue(dbs, dbnum, 9));
		db->minmulti = str2uint(PQgetvalue(dbs, dbnum, 10));
		db->next = databases;
		databases = db;

		conn = connectToServer(&old_cluster, dbname);
		res = executeQueryOrDie(conn,
								"SELECT 1 FROM pg_catalog.pg_largeobject_metadata LIMIT 1");
		if (PQntuples(res))
			pg_fatal("database \"%s\": large objects are not supported by the import prototype", dbname);
		PQclear(res);

		res = executeQueryOrDie(conn,
								"SELECT c.relname FROM pg_catalog.pg_class c "
								"JOIN pg_catalog.pg_depend d ON d.objid = c.oid "
								"WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass "
								"AND d.deptype = 'e' LIMIT 1");
		if (PQntuples(res))
			pg_fatal("database \"%s\": extension-owned relations are not supported by the import prototype", dbname);
		PQclear(res);

		res = executeQueryOrDie(conn,
								"SELECT c.relname FROM pg_catalog.pg_class c "
								"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
								"LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam "
								"WHERE c.oid >= 16384 "
								"AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema') "
								"AND n.nspname !~ '^pg_(toast_)?temp_' AND ("
								"c.relkind IN ('f', 'm', 'p') OR c.reltablespace NOT IN (0, 1663) OR "
								"EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid = c.oid) OR "
								"(c.relkind IN ('r', 'm') AND am.amname <> 'heap') OR "
								"EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a "
								"WHERE a.attrelid = c.oid AND a.attnum > 0 "
								"AND (a.attisdropped OR a.atthasmissing OR a.attgenerated <> '' OR a.atttypid >= 16384))) LIMIT 1");
		if (PQntuples(res))
			pg_fatal("database \"%s\", relation \"%s\": the import prototype requires heap storage and built-in column types",
					 dbname, PQgetvalue(res, 0, 0));
		PQclear(res);

		res = executeQueryOrDie(conn,
								"SELECT e.extname, e.extversion, n.nspname FROM pg_catalog.pg_extension e "
								"JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace");
		for (i = 0; i < PQntuples(res); i++)
		{
			const char *name = PQgetvalue(res, i, 0);
			ImportExtension *ext;

			if (strcmp(name, "plpgsql") != 0 &&
				!simple_string_list_member(&user_opts.import_extensions, name))
				pg_fatal("database \"%s\": extension \"%s\" requires --import-extension",
						 dbname, name);
			ext = pg_malloc_object(ImportExtension);
			ext->database = pg_strdup(dbname);
			ext->name = pg_strdup(name);
			ext->version = pg_strdup(PQgetvalue(res, i, 1));
			ext->schema = pg_strdup(PQgetvalue(res, i, 2));
			ext->next = extensions;
			extensions = ext;
		}
		PQclear(res);
		PQfinish(conn);
	}
	PQclear(dbs);
	check_ok();
}

/* Copy only the known SLRU file vocabulary, not arbitrary directory contents. */
void
copy_import_slru(const char *old_path, const char *new_path, bool long_names)
{
	DIR		   *dir;
	struct dirent *de;

	if (mkdir(new_path, pg_dir_create_mode) != 0)
		pg_fatal("could not create directory \"%s\": %m", new_path);
	if ((dir = opendir(old_path)) == NULL)
		pg_fatal("could not open directory \"%s\": %m", old_path);
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		size_t		len = strlen(de->d_name);
		char	   *src;
		char	   *dst;
		struct stat st;

		if ((long_names ? len != 15 : (len < 4 || len > 6)) ||
			strspn(de->d_name, "0123456789ABCDEF") != len)
			continue;
		src = psprintf("%s/%s", old_path, de->d_name);
		dst = psprintf("%s/%s", new_path, de->d_name);
		if (lstat(src, &st) != 0)
			pg_fatal("could not stat file \"%s\": %m", src);
		if (!S_ISREG(st.st_mode) || st.st_size <= 0 ||
			st.st_size > 32 * BLCKSZ || st.st_size % BLCKSZ != 0)
			pg_fatal("invalid SLRU file for physical import: \"%s\"", src);
		copyFile(src, dst, "", "SLRU");
		pg_free(src);
		pg_free(dst);
	}
	if (errno)
		pg_fatal("could not read directory \"%s\": %m", old_path);
	closedir(dir);
}

/* Source names/versions do not select code; require the provider default. */
void
check_import_target(void)
{
	PGconn	   *conn = connectToServer(&new_cluster, "template1");
	ImportExtension *ext;

	prep_status("Checking provider extension versions for import");
	for (ext = extensions; ext; ext = ext->next)
	{
		PGresult   *res;
		char	   *name = PQescapeLiteral(conn, ext->name, strlen(ext->name));

		if (!name)
			pg_fatal("could not quote extension name: %s", PQerrorMessage(conn));
		res = executeQueryOrDie(conn,
								"SELECT default_version FROM pg_catalog.pg_available_extensions "
								"WHERE name = %s", name);
		if (PQntuples(res) != 1 ||
			strcmp(PQgetvalue(res, 0, 0), ext->version) != 0)
			pg_fatal("extension \"%s\": source version must match the provider default version \"%s\"",
					 ext->name,
					 PQntuples(res) ? PQgetvalue(res, 0, 0) : "(not installed)");
		PQclear(res);
		PQfreemem(name);
	}
	PQfinish(conn);
	check_ok();
}

/* Only typed metadata reaches the privileged connection. Never archive SQL. */
void
prepare_import_databases(void)
{
	PGconn	   *conn = connectToServer(&new_cluster, "template1");
	ImportDatabase *db;
	ImportExtension *ext;

	prep_status("Creating customer databases from trusted template0");
	for (db = databases; db; db = db->next)
	{
		PQExpBufferData sql;

		initPQExpBuffer(&sql);
		appendPQExpBuffer(&sql,
						  "CREATE DATABASE %s WITH TEMPLATE template0 OWNER " IMPORT_OWNER " OID %u STRATEGY FILE_COPY ENCODING ",
						  quote_identifier(db->name), db->oid);
		import_literal(&sql, conn, db->encoding);
		switch (db->provider)
		{
			case 'b':
				appendPQExpBufferStr(&sql, " LOCALE_PROVIDER builtin");
				break;
			case 'c':
				appendPQExpBufferStr(&sql, " LOCALE_PROVIDER libc");
				break;
			case 'i':
				appendPQExpBufferStr(&sql, " LOCALE_PROVIDER icu");
				break;
			default:
				pg_fatal("unsupported locale provider in import metadata");
		}
		if (db->collate[0])
		{
			appendPQExpBufferStr(&sql, " LC_COLLATE ");
			import_literal(&sql, conn, db->collate);
		}
		if (db->ctype[0])
		{
			appendPQExpBufferStr(&sql, " LC_CTYPE ");
			import_literal(&sql, conn, db->ctype);
		}
		if (db->locale)
		{
			appendPQExpBufferStr(&sql, db->provider == 'b' ? " BUILTIN_LOCALE " : " ICU_LOCALE ");
			import_literal(&sql, conn, db->locale);
		}
		if (db->icurules)
		{
			appendPQExpBufferStr(&sql, " ICU_RULES ");
			import_literal(&sql, conn, db->icurules);
		}
		if (db->collversion)
		{
			appendPQExpBufferStr(&sql, " COLLATION_VERSION ");
			import_literal(&sql, conn, db->collversion);
		}
		PQclear(executeQueryOrDie(conn, "%s", sql.data));
		PQclear(executeQueryOrDie(conn,
								  "UPDATE pg_catalog.pg_database SET datfrozenxid = '%u', datminmxid = '%u' WHERE oid = '%u'",
								  db->frozen, db->minmulti, db->oid));
		termPQExpBuffer(&sql);
	}
	PQclear(executeQueryOrDie(conn, "CHECKPOINT"));
	PQfinish(conn);
	check_ok();

	prep_status("Installing provider extensions before customer SQL");
	for (ext = extensions; ext; ext = ext->next)
	{
		conn = connectToServer(&new_cluster, ext->database);
		if (strcmp(ext->schema, "pg_catalog") != 0 && strcmp(ext->schema, "public") != 0)
			PQclear(executeQueryOrDie(conn, "CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION " IMPORT_OWNER,
									  quote_identifier(ext->schema)));
		/* Separate fmtId calls: quote_identifier may reuse its buffer. */
		{
			char	   *schema = pg_strdup(quote_identifier(ext->schema));

			PQclear(executeQueryOrDie(conn, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s",
									  quote_identifier(ext->name), schema));
			pg_free(schema);
		}
		PQfinish(conn);
	}
	check_ok();
}

void
restore_import_schemas(void)
{
	int			dbnum;
	PGconn	   *conn;

	prep_status_progress("Restoring customer schemas as import maintenance");
	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *db = &old_cluster.dbarr.dbs[dbnum];
		PQExpBufferData connstr;
		PQExpBufferData escaped;
		char		logfile[MAXPGPATH];

		initPQExpBuffer(&connstr);
		initPQExpBuffer(&escaped);
		appendPQExpBufferStr(&connstr, "dbname=");
		appendConnStrVal(&connstr, db->db_name);
		appendPQExpBufferStr(&connstr, " user=" IMPORT_ROLE);
		appendShellString(&escaped, connstr.data);
		snprintf(logfile, sizeof(logfile), DB_DUMP_LOG_FILE_MASK, db->db_oid);
		parallel_exec_prog(logfile, NULL,
						   "\"%s/pg_restore\" %s --username=" IMPORT_ROLE " --exit-on-error --verbose "
						   "--transaction-size=1000 --dbname %s \"%s/" DB_DUMP_FILE_MASK "\"",
						   new_cluster.bindir, cluster_conn_opts(&new_cluster), escaped.data,
						   log_opts.dumpdir, db->db_oid);
		termPQExpBuffer(&connstr);
		termPQExpBuffer(&escaped);
	}
	while (reap_child(true))
		;
	end_progress_output();
	check_ok();
	conn = connectToServer(&new_cluster, "template1");
	PQclear(executeQueryOrDie(conn, "ALTER ROLE " IMPORT_ROLE " NOLOGIN"));
	PQfinish(conn);
}
