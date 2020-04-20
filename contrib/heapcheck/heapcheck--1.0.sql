/* contrib/heapcheck/heapcheck--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION heapcheck" to load this file. \quit

-- Show visibility map and page-level visibility information for each block.
CREATE FUNCTION heapcheck_relation(regclass,
								  blkno OUT bigint,
								  offnum OUT integer,
								  lp_off OUT smallint,
								  lp_flags OUT smallint,
								  lp_len OUT smallint,
								  attnum OUT integer,
								  chunk OUT integer,
								  msg OUT text
								  )
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heapcheck_relation'
LANGUAGE C STRICT;
REVOKE ALL ON FUNCTION heapcheck_relation(regclass) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION heapcheck_relation(regclass) TO pg_stat_scan_tables;
