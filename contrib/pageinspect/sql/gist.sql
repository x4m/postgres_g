CREATE TABLE test_gist AS SELECT point(i,i) p, i::text t FROM
    generate_series(1,1000) i;
CREATE INDEX test_gist_idx ON test_gist USING gist (p);

\x

SELECT * FROM gist_page_opaque_info(get_raw_page('test_gist_idx', 0));
SELECT * FROM gist_page_opaque_info(get_raw_page('test_gist_idx', 1));
SELECT * FROM gist_page_opaque_info(get_raw_page('test_gist_idx', 2));

SELECT * FROM gist_page_items(get_raw_page('test_gist_idx', 0));
SELECT * FROM gist_page_items(get_raw_page('test_gist_idx', 1)) LIMIT 10;
SELECT * FROM gist_page_items(get_raw_page('test_gist_idx', 2)) LIMIT 10;

DROP TABLE test1;
