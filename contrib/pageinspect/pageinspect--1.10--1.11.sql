/* contrib/pageinspect/pageinspect--1.10--1.11.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.11'" to load this file. \quit

--
-- Functions that fetch relation pages must be PARALLEL RESTRICTED,
-- not PARALLEL SAFE, otherwise they will fail when run on a
-- temporary table in a parallel worker process.
--

ALTER FUNCTION get_raw_page(text, int8) PARALLEL RESTRICTED;
ALTER FUNCTION get_raw_page(text, text, int8) PARALLEL RESTRICTED;
-- tuple_data_split must be restricted because it may fetch TOAST data.
ALTER FUNCTION tuple_data_split(oid, bytea, integer, integer, text) PARALLEL RESTRICTED;
ALTER FUNCTION tuple_data_split(oid, bytea, integer, integer, text, bool) PARALLEL RESTRICTED;
-- heap_page_item_attrs must be restricted because it calls tuple_data_split.
ALTER FUNCTION heap_page_item_attrs(bytea, regclass, bool) PARALLEL RESTRICTED;
ALTER FUNCTION heap_page_item_attrs(bytea, regclass) PARALLEL RESTRICTED;
ALTER FUNCTION bt_metap(text) PARALLEL RESTRICTED;
ALTER FUNCTION bt_page_stats(text, int8) PARALLEL RESTRICTED;
ALTER FUNCTION bt_page_items(text, int8) PARALLEL RESTRICTED;
ALTER FUNCTION hash_bitmap_info(regclass, int8) PARALLEL RESTRICTED;
-- brin_page_items might be parallel safe, because it seems to touch
-- only index metadata, but I don't think there's a point in risking it.
-- Likewise for gist_page_items.
ALTER FUNCTION brin_page_items(bytea, regclass) PARALLEL RESTRICTED;
ALTER FUNCTION gist_page_items(bytea, regclass) PARALLEL RESTRICTED;

--
-- gist_page_opaque_info()
--
DROP FUNCTION gist_page_opaque_info(bytea);
CREATE FUNCTION gist_page_opaque_info(IN page bytea,
    OUT lsn pg_lsn,
    OUT nsn pg_lsn,
    OUT rightlink bigint,
    OUT flags text[])
AS 'MODULE_PATHNAME', 'gist_page_opaque_info'
LANGUAGE C STRICT PARALLEL SAFE;


--
-- gist_page_items_bytea()
--
DROP FUNCTION gist_page_items_bytea(bytea);
CREATE FUNCTION gist_page_items_bytea(IN page bytea,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT dead boolean,
    OUT key_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gist_page_items_bytea'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- gist_page_items()
--
DROP FUNCTION gist_page_items(bytea, regclass);
CREATE FUNCTION gist_page_items(IN page bytea,
    IN index_oid regclass,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT dead boolean,
    OUT keys text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gist_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- get_raw_page()
--
DROP FUNCTION get_raw_page(text, int8);
DROP FUNCTION IF EXISTS get_raw_page(text, int4);
CREATE FUNCTION get_raw_page(text, int8)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

DROP FUNCTION get_raw_page(text, text, int8);
DROP FUNCTION IF EXISTS get_raw_page(text, text, int4);
CREATE FUNCTION get_raw_page(text, text, int8)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_fork_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- page_checksum()
--
DROP FUNCTION page_checksum(IN page bytea, IN blkno int8);
DROP FUNCTION IF EXISTS page_checksum(IN page bytea, IN blkno int4);
CREATE FUNCTION page_checksum(IN page bytea, IN blkno int8)
RETURNS smallint
AS 'MODULE_PATHNAME', 'page_checksum_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_metap()
--
DROP FUNCTION bt_metap(text);
CREATE FUNCTION bt_metap(IN relname text,
    OUT magic int4,
    OUT version int4,
    OUT root int8,
    OUT level int8,
    OUT fastroot int8,
    OUT fastlevel int8,
    OUT last_cleanup_num_delpages int8,
    OUT last_cleanup_num_tuples float8,
    OUT allequalimage boolean)
AS 'MODULE_PATHNAME', 'bt_metap'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_stats()
--
DROP FUNCTION bt_page_stats(text, int8);
DROP FUNCTION IF EXISTS bt_page_stats(text, int4);
CREATE FUNCTION bt_page_stats(IN relname text, IN blkno int8,
    OUT blkno int8,
    OUT type "char",
    OUT live_items int4,
    OUT dead_items int4,
    OUT avg_item_size int4,
    OUT page_size int4,
    OUT free_size int4,
    OUT btpo_prev int8,
    OUT btpo_next int8,
    OUT btpo_level int8,
    OUT btpo_flags int4)
AS 'MODULE_PATHNAME', 'bt_page_stats_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_items()
--
DROP FUNCTION bt_page_items(text, int8);
DROP FUNCTION IF EXISTS bt_page_items(text, int4);
CREATE FUNCTION bt_page_items(IN relname text, IN blkno int8,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items_1_9'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- brin_page_items()
--
DROP FUNCTION brin_page_items(IN page bytea, IN index_oid regclass);
CREATE FUNCTION brin_page_items(IN page bytea, IN index_oid regclass,
    OUT itemoffset int,
    OUT blknum int8,
    OUT attnum int,
    OUT allnulls bool,
    OUT hasnulls bool,
    OUT placeholder bool,
    OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'brin_page_items'
LANGUAGE C STRICT PARALLEL SAFE;
