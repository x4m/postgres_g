/* src/test/modules/test_multixact/test_multixact--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_multixact" to load this file. \quit

CREATE FUNCTION test_multixact(concurrency integer DEFAULT 3,
    scale integer DEFAULT 65536,
    debug_output boolean DEFAULT false)
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
