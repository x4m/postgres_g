/* src/test/modules/test_blockset/test_blockset--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_blockset" to load this file. \quit

CREATE FUNCTION test_blockset()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
