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
