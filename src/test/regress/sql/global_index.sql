--
-- GLOBAL index tests
--
CREATE TABLE range_parted (
	a int,
	b int
) PARTITION BY RANGE (a);

--create some partitions and insert data
CREATE TABLE range_parted_1 PARTITION OF range_parted FOR VALUES FROM (1) TO (100000);
CREATE TABLE range_parted_2 PARTITION OF range_parted FOR VALUES FROM (100000) TO (200000);
CREATE TABLE range_parted_3 PARTITION OF range_parted FOR VALUES FROM (200000) TO (300000);
INSERT INTO range_parted SELECT i,i%100 FROM generate_series(1,299999) AS i;

--Create global index
CREATE INDEX global_idx ON range_parted(b) global;
INSERT INTO range_parted SELECT i,i%200 FROM generate_series(1,299999) AS i;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 7;
SELECT * FROM range_parted WHERE b = 7 LIMIT 10;

EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 110;
SELECT * FROM range_parted WHERE b = 110 LIMIT 10;
SELECT * FROM range_parted WHERE b = 250 LIMIT 10;

UPDATE range_parted SET b=b+100;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 250;
SELECT * FROM range_parted WHERE b = 250 LIMIT 10;

--attach partition
CREATE TABLE range_parted_4(a int, b int);
INSERT INTO range_parted_4 SELECT i,i%300 + 100 FROM generate_series(300000,300300) as i;
ALTER TABLE range_parted ATTACH PARTITION range_parted_4 FOR VALUES FROM (300000) to (400000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 350;
SELECT * FROM range_parted WHERE b = 350 LIMIT 10;

CREATE TABLE range_parted_5(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE range_parted_5_1 PARTITION OF range_parted_5 FOR VALUES FROM (400000) TO (450000);
CREATE TABLE range_parted_5_2 PARTITION OF range_parted_5 FOR VALUES FROM (450000) TO (460000);
INSERT INTO range_parted_5 SELECT i,i%100 + 500 FROM generate_series(400000,459999) AS i;
CREATE INDEX global_idx_1 ON range_parted_5(b) global;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 550;
SELECT * FROM range_parted_5 WHERE b = 550 LIMIT 5;

--attach to the top partition
ALTER TABLE range_parted ATTACH PARTITION range_parted_5 FOR VALUES FROM (400000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

-- Check index only scan
EXPLAIN (COSTS OFF) SELECT b FROM range_parted WHERE b = 550 LIMIT 5;
SELECT b FROM range_parted WHERE b = 550 LIMIT 5;

-- Attach to level-1 partition (test with multi level global index)
CREATE TABLE range_parted_6(a int, b int);
INSERT INTO range_parted_6 SELECT i,i%100 + 600 FROM generate_series(460000,490000) AS i;
ALTER TABLE range_parted_5 ATTACH PARTITION range_parted_6 FOR VALUES FROM (460000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 650;
SELECT * FROM range_parted WHERE b = 650 LIMIT 5;

-- Update the leaf and check we are inserting that in multi-level global index
UPDATE range_parted SET b=b+1000;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 1650;
--SELECT * FROM range_parted WHERE b = 1650 LIMIT 5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 1650;
--SELECT * FROM range_parted_5 WHERE b = 1650 LIMIT 5;

-- Conditional update using global index
EXPLAIN (COSTS OFF) UPDATE range_parted SET b=b+1000 where b = 1650;
UPDATE range_parted SET b=b+1000 where b = 1650;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted_5 WHERE b = 2650;
--SELECT * FROM range_parted_5 WHERE b = 2650 LIMIT 5;

--Detach partition
ALTER TABLE range_parted DETACH PARTITION range_parted_5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

--Reattach the partition
ALTER TABLE range_parted ATTACH PARTITION range_parted_5 FOR VALUES FROM (400000) to (500000);
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

--Drop the partitioned table
DROP TABLE range_parted_5;
EXPLAIN (COSTS OFF) SELECT * FROM range_parted WHERE b = 550;
SELECT * FROM range_parted WHERE b = 550 LIMIT 5;

-- Test unique global index
TRUNCATE TABLE range_parted;
INSERT INTO range_parted VALUES(1,2);
INSERT INTO range_parted VALUES(2,2);
CREATE UNIQUE INDEX global_idx_unique ON range_parted(b) global; -- Fail with duplicate
TRUNCATE TABLE range_parted;
INSERT INTO range_parted VALUES(1,2);
CREATE UNIQUE INDEX global_idx_unique ON range_parted(b) global;
INSERT INTO range_parted VALUES(1,2); -- Fail with duplicate
DROP INDEX global_idx_unique;
INSERT INTO range_parted VALUES(1,2); -- Now this should pass

-- multiple level multiple type partitions
CREATE TABLE parent_table (
    id INT,
    category TEXT,
    sub_category TEXT,
    value INT
) PARTITION BY RANGE (id);


DO $$
DECLARE
    range_start INT;
    range_end INT;
    range_partition_name TEXT;
    list_partition_name TEXT;
    hash_partition_name TEXT;
    i INT;
    j INT;
    k INT;
BEGIN
    -- Create range partitions
    FOR i IN 0..10 LOOP
        range_start := i * 1000;
        range_end := (i + 1) * 1000;
        range_partition_name := format('parent_table_%s', i);
        EXECUTE format('CREATE TABLE %I PARTITION OF parent_table FOR VALUES FROM (%s) TO (%s) PARTITION BY LIST(category)', range_partition_name, range_start, range_end);

        -- Create list partitions within each range partition
        FOR j IN 1..10 LOOP
            list_partition_name := format('%s_list_%s', range_partition_name, j);
            EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''%s'') PARTITION BY HASH (id)', list_partition_name, range_partition_name, j);

            -- Create hash partitions within each list partition
            FOR k IN 0..4 LOOP
                hash_partition_name := format('%s_hash_%s', list_partition_name, k);
                EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES WITH (MODULUS 5, REMAINDER %s)', hash_partition_name, list_partition_name, k);
            END LOOP;
        END LOOP;
    END LOOP;
END $$;


DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 10000 LOOP
        INSERT INTO parent_table (id, category, sub_category, value)
        VALUES (i, '' || (i % 10 + 1), '' || (i % 10 + 1), i);
        i := i + 1;
    END LOOP;
END $$;

CREATE INDEX global_index_v ON parent_table(value) global;
EXPLAIN (COSTS OFF) SELECT * FROM parent_table WHERE value = 9000;
SELECT * FROM parent_table WHERE value = 9000;

-- Cleanup
DROP TABLE range_parted;
