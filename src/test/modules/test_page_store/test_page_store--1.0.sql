\echo Use "CREATE EXTENSION test_page_store" to load this file. \quit

-- This is a test interface, not a proposed SQL or storage protocol API.
CREATE FUNCTION test_page_store_read(
    relation regclass,
    expected_system_identifier text,
    expected_timeline bigint,
    expected_replay_lsn pg_lsn,
    block_number bigint,
    OUT tablespace oid,
    OUT database oid,
    OUT relfilenumber oid,
    OUT nblocks bigint,
    OUT page bytea)
RETURNS record
AS 'MODULE_PATHNAME', 'test_page_store_read'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION
test_page_store_read(regclass, text, bigint, pg_lsn, bigint) FROM PUBLIC;

CREATE FUNCTION test_page_store_fetch(
    tablespace oid, database oid, relfilenumber oid, fork_number integer,
    block_number bigint, block_count integer,
    expected_system_identifier text, expected_timeline bigint,
    expected_replay_lsn pg_lsn,
    OUT fork_exists boolean, OUT nblocks bigint, OUT pages bytea)
RETURNS record
AS 'MODULE_PATHNAME', 'test_page_store_fetch'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION test_page_store_fetch(oid, oid, oid, integer, bigint,
    integer, text, bigint, pg_lsn) FROM PUBLIC;

CREATE FUNCTION test_page_store_io_counts(OUT readv bigint, OUT startreadv bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'test_page_store_io_counts'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION test_page_store_read_smgr(regclass, integer)
RETURNS bytea
AS 'MODULE_PATHNAME', 'test_page_store_read_smgr'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION test_page_store_read_smgr(regclass, integer) FROM PUBLIC;
