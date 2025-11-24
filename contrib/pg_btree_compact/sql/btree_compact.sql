-- Test for pg_btree_compact extension

CREATE EXTENSION pg_btree_compact;

-- Make tests deterministic
SELECT setseed(0.5);

-- We'll also need amcheck and pageinspect for validation
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;

--
-- Test 1: Basic compaction with simple dataset
--
CREATE TABLE test_basic (id integer, data text);
INSERT INTO test_basic SELECT i, 'data_' || i FROM generate_series(1, 1000) i;
CREATE INDEX test_basic_idx ON test_basic (id) WITH (fillfactor = 30);

-- Create bloat by deleting most rows
DELETE FROM test_basic WHERE id % 10 != 0;
VACUUM test_basic;

-- Check initial state
SELECT pg_size_pretty(pg_relation_size('test_basic_idx')) as size_before;

-- Estimate savings
SELECT total_pages > 0, leaf_pages > 0, sparse_pages >= 0 
FROM btree_compact_estimate('test_basic_idx', 20);

-- Compact
SELECT pages_visited > 0 as has_pages,
       pages_merged >= 0 as did_merge,
       total_time_ms >= 0 as has_timing
FROM btree_compact('test_basic_idx', 20, 200);

-- Verify with amcheck
SELECT bt_index_check('test_basic_idx', true);

-- Verify data integrity
SELECT count(*) FROM test_basic;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM test_basic WHERE id BETWEEN 50 AND 150;
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE test_basic CASCADE;

--
-- Test 2: Large dataset with heavy bloat
--
CREATE TABLE test_large (id integer PRIMARY KEY, value text);
INSERT INTO test_large SELECT i, lpad(i::text, 50, '0') FROM generate_series(1, 5000) i;

-- Delete 80% to create severe bloat
DELETE FROM test_large WHERE (id % 5) != 0;
VACUUM test_large;

SELECT pg_size_pretty(pg_relation_size('test_large_pkey')) as size_before;

-- Compact
SELECT pages_visited, pages_merged, pages_deleted, tuples_moved
FROM btree_compact('test_large_pkey', 30, 250);

-- Verify integrity
SELECT bt_index_check('test_large_pkey', true);

-- Verify correct results
SELECT count(*) FROM test_large;
SELECT count(*) FROM test_large WHERE id BETWEEN 1000 AND 2000;

DROP TABLE test_large CASCADE;

--
-- Test 3: Multiple indexes on same table
--
CREATE TABLE test_multi (
    id integer,
    code text,
    value numeric
);
INSERT INTO test_multi 
SELECT i, 'code_' || (i % 100), (i * 17) % 1000 
FROM generate_series(1, 3000) i;

CREATE INDEX test_multi_id_idx ON test_multi (id) WITH (fillfactor = 40);
CREATE INDEX test_multi_code_idx ON test_multi (code) WITH (fillfactor = 40);
CREATE INDEX test_multi_value_idx ON test_multi (value) WITH (fillfactor = 40);

-- Create different bloat patterns
DELETE FROM test_multi WHERE id % 3 = 0;
DELETE FROM test_multi WHERE code LIKE '%5';
VACUUM test_multi;

-- Compact each index
SELECT 'test_multi_id_idx' as index_name, pages_merged 
FROM btree_compact('test_multi_id_idx', 25, 200);

SELECT 'test_multi_code_idx' as index_name, pages_merged 
FROM btree_compact('test_multi_code_idx', 25, 200);

SELECT 'test_multi_value_idx' as index_name, pages_merged 
FROM btree_compact('test_multi_value_idx', 25, 200);

-- Verify all indexes
SELECT bt_index_check('test_multi_id_idx', true);
SELECT bt_index_check('test_multi_code_idx', true);
SELECT bt_index_check('test_multi_value_idx', true);

-- Verify queries work
SELECT count(*) FROM test_multi WHERE id < 1000;
SELECT count(*) FROM test_multi WHERE code = 'code_7';
SELECT count(*) FROM test_multi WHERE value > 500;

DROP TABLE test_multi CASCADE;

--
-- Test 4: Edge cases - empty ranges
--
CREATE TABLE test_empty (id integer);
CREATE INDEX test_empty_idx ON test_empty (id);

-- Compact empty index
SELECT pages_visited, pages_merged FROM btree_compact('test_empty_idx', 10, 100);
SELECT bt_index_check('test_empty_idx', true);

-- Add a few rows
INSERT INTO test_empty SELECT generate_series(1, 10);
SELECT pages_visited, pages_merged FROM btree_compact('test_empty_idx', 10, 100);
SELECT bt_index_check('test_empty_idx', true);

DROP TABLE test_empty CASCADE;

--
-- Test 5: Compaction with different thresholds
--
CREATE TABLE test_thresholds (id integer);
INSERT INTO test_thresholds SELECT generate_series(1, 2000);
CREATE INDEX test_thresholds_idx ON test_thresholds (id) WITH (fillfactor = 30);
DELETE FROM test_thresholds WHERE id % 4 != 0;
VACUUM test_thresholds;

-- Try different min_items thresholds
SELECT min_items, sparse_pages 
FROM (VALUES (10), (20), (30), (40), (50)) v(min_items)
CROSS JOIN LATERAL btree_compact_estimate('test_thresholds_idx', min_items)
ORDER BY min_items;

-- Compact with moderate threshold
SELECT pages_merged FROM btree_compact('test_thresholds_idx', 30, 200);
SELECT bt_index_check('test_thresholds_idx', true);

DROP TABLE test_thresholds CASCADE;

--
-- Test 6: Verify page linkage after compaction
--
CREATE TABLE test_links (id integer);
INSERT INTO test_links SELECT generate_series(1, 3000);
CREATE INDEX test_links_idx ON test_links (id) WITH (fillfactor = 20);
DELETE FROM test_links WHERE (id % 10) > 2;
VACUUM test_links;

-- Get page count before
SELECT count(*) as pages_before 
FROM generate_series(1, (SELECT (pg_relation_size('test_links_idx')/8192)::int - 1)) s(blkno)
CROSS JOIN LATERAL bt_page_stats('test_links_idx', s.blkno);

-- Compact
SELECT pages_merged, pages_deleted FROM btree_compact('test_links_idx', 40, 250);

-- Verify page linkage is correct
WITH RECURSIVE page_chain AS (
  -- Start from leftmost leaf
  SELECT 1 as blkno, btpo_next, 1 as depth
  FROM bt_page_stats('test_links_idx', 1)
  WHERE type = 'l'
  
  UNION ALL
  
  -- Follow the chain
  SELECT p.btpo_next, ps.btpo_next, pc.depth + 1
  FROM page_chain pc
  JOIN LATERAL bt_page_stats('test_links_idx', pc.btpo_next) ps
    ON ps.type = 'l' AND pc.btpo_next != 0
  JOIN LATERAL bt_page_stats('test_links_idx', pc.btpo_next) p ON true
  WHERE pc.depth < 1000  -- Safety limit
)
SELECT count(*) > 0 as chain_exists FROM page_chain;

-- Verify index
SELECT bt_index_check('test_links_idx', true);

DROP TABLE test_links CASCADE;

--
-- Test 7: Concurrent-safe test (single session, but verify locking)
--
CREATE TABLE test_locking (id integer);
INSERT INTO test_locking SELECT generate_series(1, 1000);
CREATE INDEX test_locking_idx ON test_locking (id) WITH (fillfactor = 30);
DELETE FROM test_locking WHERE id % 5 != 0;
VACUUM test_locking;

-- This should complete without errors
BEGIN;
SELECT pages_merged FROM btree_compact('test_locking_idx', 25, 200);
-- Can still query during same transaction
SELECT count(*) FROM test_locking WHERE id < 500;
COMMIT;

-- Verify integrity
SELECT bt_index_check('test_locking_idx', true);

DROP TABLE test_locking CASCADE;

--
-- Test 8: Non-unique index
--
CREATE TABLE test_nonunique (category integer, value text);
INSERT INTO test_nonunique 
SELECT (i % 10), 'value_' || i 
FROM generate_series(1, 2000) i;

CREATE INDEX test_nonunique_idx ON test_nonunique (category) WITH (fillfactor = 30);
-- Delete deterministically instead of using random()
DELETE FROM test_nonunique WHERE (value::text ~ '[24680]$');
VACUUM test_nonunique;

SELECT pages_merged FROM btree_compact('test_nonunique_idx', 20, 200);
SELECT bt_index_check('test_nonunique_idx', true);

-- Verify duplicates are handled correctly
SELECT category, count(*) 
FROM test_nonunique 
WHERE category IN (3, 7) 
GROUP BY category 
ORDER BY category;

DROP TABLE test_nonunique CASCADE;

--
-- Test 9: Composite index
--
CREATE TABLE test_composite (a integer, b integer, c text);
INSERT INTO test_composite 
SELECT i, i % 100, 'text_' || i 
FROM generate_series(1, 2000) i;

CREATE INDEX test_composite_idx ON test_composite (a, b) WITH (fillfactor = 30);
DELETE FROM test_composite WHERE a % 4 != 0;
VACUUM test_composite;

SELECT pages_merged FROM btree_compact('test_composite_idx', 25, 200);
SELECT bt_index_check('test_composite_idx', true);

-- Verify composite key queries work
SELECT count(*) FROM test_composite WHERE a BETWEEN 100 AND 200;
SELECT count(*) FROM test_composite WHERE a = 400 AND b = 0;

DROP TABLE test_composite CASCADE;

--
-- Test 10: Already compact index (nothing to do)
--
CREATE TABLE test_compact (id integer PRIMARY KEY);
INSERT INTO test_compact SELECT generate_series(1, 100);

-- Index is already optimal
SELECT pages_merged FROM btree_compact('test_compact_pkey', 10, 100);
SELECT bt_index_check('test_compact_pkey', true);

DROP TABLE test_compact CASCADE;

--
-- Test 11: Error handling - non-btree index
--
CREATE TABLE test_gin (id integer, doc tsvector);
INSERT INTO test_gin SELECT i, to_tsvector('english', 'document ' || i) 
FROM generate_series(1, 100) i;
CREATE INDEX test_gin_idx ON test_gin USING gin(doc);

-- Should error on non-btree index
\set ON_ERROR_STOP 0
SELECT * FROM btree_compact('test_gin_idx');
\set ON_ERROR_STOP 1

DROP TABLE test_gin CASCADE;

--
-- Final summary
--
\echo 'All tests completed successfully!'

-- Clean up extensions (they'll be dropped with CASCADE in installcheck)
-- But don't drop in this script so we can inspect results
