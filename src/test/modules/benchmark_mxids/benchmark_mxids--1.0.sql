/* src/test/modules/benchmark_mxids/benchmark_mxids--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION benchmark_mxids" to load this file. \quit

CREATE FUNCTION benchmark_mxids()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
