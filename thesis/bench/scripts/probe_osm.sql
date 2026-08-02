-- Запросы к реальным данным.
--
-- На реальных данных случайная точка из ограничивающего прямоугольника почти
-- всегда попадает в пустое место: узлы OSM сосредоточены в городах, а
-- прямоугольник покрывает и море, и поля. Такой зонд измерял бы стоимость
-- промаха, а не стоимость работы. Поэтому зонды берутся из самих данных:
-- окрестность существующего объекта. Это же соответствует практической
-- нагрузке «найти соседей данного объекта».
--
-- Вторая нагрузка — покрытие: ограничивающий прямоугольник делится сеткой, и
-- каждая ячейка запрашивается один раз. Она измеряет стоимость обхода всех
-- данных через индекс и не зависит от того, куда попали случайные зонды.

-- Окрестность случайно выбранных существующих точек.
CREATE OR REPLACE FUNCTION probe_at_data(tbl text, idx text, nprobes int, d float8)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
  r record; js json; total bigint := 0; n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR r IN EXECUTE format(
      'SELECT (p)[0] AS x, (p)[1] AS y FROM %I TABLESAMPLE SYSTEM_ROWS(%s)', tbl, nprobes)
  LOOP
    EXECUTE format(
      'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
      'SELECT count(*) FROM %I WHERE p <@ box(point(%s,%s), point(%s,%s))',
      tbl, r.x - d, r.y - d, r.x + d, r.y + d) INTO js;
    total := total
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
    n := n + 1;
  END LOOP;
  RETURN round(total::numeric / greatest(n, 1), 2);
END $$;

-- Покрытие сеткой: ограничивающий прямоугольник делится на g x g ячеек,
-- запрашивается каждая. Возвращает среднее число обращений на ячейку и
-- суммарное — стоимость полного обхода данных через индекс.
CREATE OR REPLACE FUNCTION probe_grid(tbl text, idx text, g int)
RETURNS TABLE(cells int, blocks_total bigint, blocks_avg numeric)
LANGUAGE plpgsql AS $$
DECLARE
  minx float8; miny float8; maxx float8; maxy float8;
  i int; j int; js json; total bigint := 0; n int := 0;
  dx float8; dy float8;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  EXECUTE format('SELECT min((p)[0]), min((p)[1]), max((p)[0]), max((p)[1]) FROM %I', tbl)
     INTO minx, miny, maxx, maxy;
  dx := (maxx - minx) / g;
  dy := (maxy - miny) / g;
  FOR i IN 0 .. g - 1 LOOP
    FOR j IN 0 .. g - 1 LOOP
      EXECUTE format(
        'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
        'SELECT count(*) FROM %I WHERE p <@ box(point(%s,%s), point(%s,%s))',
        tbl, minx + i * dx, miny + j * dy, minx + (i + 1) * dx, miny + (j + 1) * dy)
        INTO js;
      total := total
             + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
             + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
      n := n + 1;
    END LOOP;
  END LOOP;
  RETURN QUERY SELECT n, total, round(total::numeric / greatest(n, 1), 1);
END $$;
