-- Число обращений к страницам индекса на один точечный запрос.
-- Окно масштабируется как 0,5/sqrt(N), чтобы ожидаемое число совпадений не
-- зависело от объёма набора. Считается по EXPLAIN BUFFERS: статистика в
-- разделяемой памяти в этой версии ещё отсутствует, а коллектор отдаёт
-- значения с задержкой.
CREATE OR REPLACE FUNCTION probe_index(tbl text, idx text, nprobes int, n_rows float8)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
  i int; x float8; y float8; d float8 := 0.5 / sqrt(n_rows);
  js json; total bigint := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR i IN 1 .. nprobes LOOP
    x := random(); y := random();
    EXECUTE format(
      'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
      'SELECT count(*) FROM %I WHERE p <@ box(point(%s,%s), point(%s,%s))',
      tbl, x - d, y - d, x + d, y + d) INTO js;
    total := total
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
  END LOOP;
  RETURN round(total::numeric / nprobes, 2);
END $$;
