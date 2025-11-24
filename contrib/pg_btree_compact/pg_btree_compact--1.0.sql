/* contrib/pg_btree_compact/pg_btree_compact--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_btree_compact" to load this file. \quit

-- Main compaction function
CREATE FUNCTION btree_compact(
    index_name regclass,
    min_items_per_page integer DEFAULT 10,
    max_items_per_page integer DEFAULT NULL
) RETURNS TABLE (
    pages_visited bigint,
    pages_merged bigint,
    pages_deleted bigint,
    tuples_moved bigint,
    lock_time_ms bigint,
    total_time_ms bigint
)
AS 'MODULE_PATHNAME', 'btree_compact'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- Helper function to estimate potential space savings
CREATE FUNCTION btree_compact_estimate(
    index_name regclass,
    min_items_per_page integer DEFAULT 10
) RETURNS TABLE (
    total_pages bigint,
    leaf_pages bigint,
    sparse_pages bigint,
    estimated_savings_bytes bigint,
    estimated_savings_percent numeric
)
AS 'MODULE_PATHNAME', 'btree_compact_estimate'
LANGUAGE C STRICT PARALLEL SAFE;

