setup
{
  CREATE EXTENSION IF NOT EXISTS pageinspect;
  CREATE TABLE gist_predicate_delete (id integer, p point);
  INSERT INTO gist_predicate_delete
  SELECT g, point(g, 0) FROM generate_series(1, 20000) AS g;
  INSERT INTO gist_predicate_delete
  SELECT g, point(1000000 + g, 0)
  FROM generate_series(20001, 40000) AS g;
  CREATE INDEX gist_predicate_delete_idx
    ON gist_predicate_delete USING gist (p);
  DELETE FROM gist_predicate_delete WHERE id <= 20000;
}

teardown
{
  DROP TABLE gist_predicate_delete;
}

session s1
setup
{
  BEGIN ISOLATION LEVEL SERIALIZABLE;
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
}
step rA1 { SELECT count(*) FROM gist_predicate_delete
           WHERE p ~= point(10000.5, 0); }
step wB1 { INSERT INTO gist_predicate_delete
           VALUES (50001, point(1010000.5, 0)); }
step c1 { COMMIT; }

session s2
setup
{
  BEGIN ISOLATION LEVEL SERIALIZABLE;
  SET enable_seqscan = off;
  SET enable_bitmapscan = off;
}
step rB2 { SELECT count(*) FROM gist_predicate_delete
           WHERE p ~= point(1010000.5, 0); }
step wA2 { INSERT INTO gist_predicate_delete
           VALUES (50002, point(10000.5, 0)); }
step c2 { COMMIT; }

session maint
step vac { VACUUM gist_predicate_delete; }
step locks { SELECT locktype, page, relation::regclass
             FROM pg_locks
             WHERE mode = 'SIReadLock'
             ORDER BY relation::regclass::text, locktype, page; }
step pages { SELECT count(*) FILTER (WHERE flags @> ARRAY['deleted']) AS deleted,
                    count(*) FILTER (WHERE flags @> ARRAY['leaf']) AS leaf
             FROM generate_series(
                    1,
                    pg_relation_size('gist_predicate_delete_idx') / 8192 - 1
                  ) AS g(blkno),
                  LATERAL gist_page_opaque_info(
                    get_raw_page('gist_predicate_delete_idx', blkno)) AS p; }
step lockedpages { SELECT DISTINCT l.page, p.flags
                   FROM pg_locks AS l,
                        LATERAL gist_page_opaque_info(
                          get_raw_page('gist_predicate_delete_idx', l.page)
                        ) AS p
                   WHERE l.mode = 'SIReadLock'
                     AND l.relation = 'gist_predicate_delete_idx'::regclass
                   ORDER BY l.page; }
step destination { SELECT blkno, itemoffset, keys
                   FROM generate_series(
                          1,
                          pg_relation_size('gist_predicate_delete_idx') / 8192 - 1
                        ) AS g(blkno),
                        LATERAL gist_page_opaque_info(
                          get_raw_page('gist_predicate_delete_idx', blkno)) AS o,
                        LATERAL gist_page_items(
                          get_raw_page('gist_predicate_delete_idx', blkno),
                          'gist_predicate_delete_idx') AS i
                   WHERE o.flags @> ARRAY['leaf']
                     AND NOT o.flags @> ARRAY['deleted']
                     AND i.keys LIKE '%10000.5%'; }

# Control: while the dead index tuples remain, SSI must detect the cycle.
permutation rA1 rB2 wA2 wB1 c1 c2

# VACUUM can delete the page carrying rA1's SIREAD lock.
permutation rA1 rB2 locks vac pages lockedpages wA2 destination wB1 c1 c2
