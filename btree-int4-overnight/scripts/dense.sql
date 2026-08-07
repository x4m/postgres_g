\set k random(1, 5000000)
SELECT k FROM dense WHERE k = :k;
