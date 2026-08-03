-- Доля повторных обращений к страницам индекса.
--
-- От этого числа зависит, имеет ли смысл внутристраничная структура в памяти:
-- её построение стоит примерно столько же, сколько экономит при однократном
-- просмотре страницы, поэтому выигрыш возможен только при повторных
-- обращениях к одной и той же странице.
--
-- Считаем точно, а не по косвенным признакам вроде usagecount. Ключи узлов
-- восстановлены снизу вверх (omega_levels.sql), поэтому для каждого зонда
-- известно в точности, какие узлы каждого уровня он посещает: те, чей ключ его
-- накрывает. Отсюда:
--
--   всего посещений   = сколько раз узлы уровня попали хоть в один зонд
--   различных страниц = сколько различных узлов уровня посещено
--   доля повторов     = 1 - различных / всего
--
-- Требует, чтобы gist_node_boxes из omega_levels.sql был уже загружен.

DROP FUNCTION IF EXISTS revisit_stats(text, int);
CREATE FUNCTION revisit_stats(idx text, nprobes int DEFAULT 2000)
RETURNS TABLE(lvl int, visits bigint, distinct_pages bigint,
              visits_per_page numeric, revisit_share numeric)
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _nb(blk int, lvl int, bbox box) ON COMMIT DROP;
  DELETE FROM _nb;
  INSERT INTO _nb SELECT * FROM gist_node_boxes(idx);

  CREATE TEMP TABLE IF NOT EXISTS _q(p point) ON COMMIT DROP;
  DELETE FROM _q;
  INSERT INTO _q SELECT point(random(), random()) FROM generate_series(1, nprobes);

  RETURN QUERY
    SELECT n.lvl,
           count(*) FILTER (WHERE q.p <@ n.bbox)::bigint AS visits,
           count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox)::bigint AS distinct_pages,
           round(count(*) FILTER (WHERE q.p <@ n.bbox)::numeric
                 / greatest(count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox), 1), 2),
           round(1 - count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox)::numeric
                     / greatest(count(*) FILTER (WHERE q.p <@ n.bbox), 1), 3)
      FROM _nb n CROSS JOIN _q q
     GROUP BY n.lvl
     ORDER BY n.lvl;
END $$;

-- То же для соединения по индексу: зонды берутся из отдельной таблицы, чтобы
-- повторить нагрузку, на которой снят профиль процессора.
DROP FUNCTION IF EXISTS revisit_stats_join(text, text);
CREATE FUNCTION revisit_stats_join(idx text, probe_tbl text)
RETURNS TABLE(lvl int, visits bigint, distinct_pages bigint,
              visits_per_page numeric, revisit_share numeric)
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _nb(blk int, lvl int, bbox box) ON COMMIT DROP;
  DELETE FROM _nb;
  INSERT INTO _nb SELECT * FROM gist_node_boxes(idx);

  CREATE TEMP TABLE IF NOT EXISTS _q(p point) ON COMMIT DROP;
  DELETE FROM _q;
  EXECUTE format('INSERT INTO _q SELECT point(x, y) FROM %I', probe_tbl);

  RETURN QUERY
    SELECT n.lvl,
           count(*) FILTER (WHERE q.p <@ n.bbox)::bigint,
           count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox)::bigint,
           round(count(*) FILTER (WHERE q.p <@ n.bbox)::numeric
                 / greatest(count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox), 1), 2),
           round(1 - count(DISTINCT n.blk) FILTER (WHERE q.p <@ n.bbox)::numeric
                     / greatest(count(*) FILTER (WHERE q.p <@ n.bbox), 1), 3)
      FROM _nb n CROSS JOIN _q q
     GROUP BY n.lvl
     ORDER BY n.lvl;
END $$;
