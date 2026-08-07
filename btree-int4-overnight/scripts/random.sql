\set n random(1, 5000000)
SELECT k FROM random_keys
WHERE k = ((:n::bigint * 15485863) % 2147483647)::int4;
