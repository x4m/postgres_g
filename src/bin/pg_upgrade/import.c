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
	char	   *name;
	char	   *version;
	struct ImportExtension *next;
} ImportExtension;

static ImportExtension *extensions;
static char *import_config;

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
	res = executeQueryOrDie(conn,
							"SELECT 1 FROM pg_catalog.pg_tablespace "
							"WHERE oid NOT IN (1663, 1664)");
	if (PQntuples(res))
		pg_fatal("the import prototype does not support user tablespaces");
	PQclear(res);
	dbs = executeQueryOrDie(conn,
							"SELECT datname FROM pg_catalog.pg_database "
							"WHERE datallowconn ORDER BY datname");
	PQfinish(conn);

	for (dbnum = 0; dbnum < PQntuples(dbs); dbnum++)
	{
		const char *dbname = PQgetvalue(dbs, dbnum, 0);
		int			i;

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
								"c.relkind = 'f' OR "
								"(c.relkind IN ('r', 'm') AND am.amname <> 'heap') OR "
								"EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a "
								"WHERE a.attrelid = c.oid AND a.attnum > 0 "
								"AND NOT a.attisdropped AND a.atttypid >= 16384)) LIMIT 1");
		if (PQntuples(res))
			pg_fatal("database \"%s\", relation \"%s\": the import prototype requires heap storage and built-in column types",
					 dbname, PQgetvalue(res, 0, 0));
		PQclear(res);

		res = executeQueryOrDie(conn,
								"SELECT extname, extversion FROM pg_catalog.pg_extension");
		for (i = 0; i < PQntuples(res); i++)
		{
			const char *name = PQgetvalue(res, i, 0);
			ImportExtension *ext;

			if (strcmp(name, "plpgsql") != 0 &&
				!simple_string_list_member(&user_opts.import_extensions, name))
				pg_fatal("database \"%s\": extension \"%s\" requires --import-extension",
						 dbname, name);
			ext = pg_malloc_object(ImportExtension);
			ext->name = pg_strdup(name);
			ext->version = pg_strdup(PQgetvalue(res, i, 1));
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
