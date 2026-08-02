-- Данные с управляемым размером кластера.
--
-- Гипотеза (plan/60-pvldb.md, п. 2): при разбиении по заполнению страницы
-- перекрытие нелистовых ключей максимально, когда размер плотной области
-- систематически не совпадает с ёмкостью страницы, и падает, когда кратен ей.
-- Управляющий параметр — c = (точек в кластере) / (ёмкость страницы f).

CREATE EXTENSION IF NOT EXISTS pageinspect;

-- n_clusters кластеров по per_cluster точек, диаметр кластера sigma.
CREATE OR REPLACE FUNCTION gen_clustered(n_clusters int, per_cluster int, sigma float8)
RETURNS SETOF point LANGUAGE sql AS $$
  SELECT point(c.cx + sigma * (random() - 0.5),
               c.cy + sigma * (random() - 0.5))
    FROM (SELECT random() AS cx, random() AS cy
            FROM generate_series(1, n_clusters)) c,
         LATERAL generate_series(1, per_cluster) g;
$$;

-- Равномерное распределение для контрольной точки.
CREATE OR REPLACE FUNCTION gen_uniform(n int)
RETURNS SETOF point LANGUAGE sql AS $$
  SELECT point(random(), random()) FROM generate_series(1, n);
$$;

-- Ёмкость страницы измеряется, а не вычисляется: зависит от версии и от того,
-- как класс операторов представляет ключ.
CREATE OR REPLACE FUNCTION measure_fanout(idx text)
RETURNS TABLE(lvl int, pages bigint, items_avg numeric) LANGUAGE plpgsql AS $$
DECLARE
  nblocks bigint := pg_relation_size(idx::regclass) / 8192;
  b bigint;
  lv int;
  cnt int;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _fanout(lvl int, items int) ON COMMIT DROP;
  DELETE FROM _fanout;
  FOR b IN 1 .. nblocks - 1 LOOP   -- 0 — метастраница
    BEGIN
      SELECT o.level INTO lv
        FROM gist_page_opaque_info(get_raw_page(idx, b::int)) o;
      SELECT count(*) INTO cnt
        FROM gist_page_items(get_raw_page(idx, b::int), idx::regclass);
      INSERT INTO _fanout VALUES (lv, cnt);
    EXCEPTION WHEN OTHERS THEN
      NULL;  -- пустые и удалённые страницы пропускаем
    END;
  END LOOP;
  RETURN QUERY SELECT f.lvl, count(*)::bigint, round(avg(f.items), 1)
                 FROM _fanout f GROUP BY f.lvl ORDER BY f.lvl;
END $$;
