\set k random(0, 9999)
SELECT k FROM duplicates WHERE k = :k LIMIT 1;
