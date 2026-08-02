-- Поуровневое omega_l без разбора сырых байтов.
--
-- Прямо прочитать внутренние ключи нельзя: pageinspect версии 15 декодирует их
-- дескриптором листовой строки, и box печатается как point. Но ключ узла в
-- обобщённом дереве поиска по определению покрывает ключи поддерева, а при
-- построении сортировкой он равен в точности их объединению. Поэтому ключ
-- восстанавливается снизу вверх: для листа — описывающий прямоугольник его
-- точек (листовые ключи декодируются верно), для внутреннего узла —
-- объединение ключей потомков. Топология берётся из ctid, который ошибкой
-- декодирования не затронут.

CREATE EXTENSION IF NOT EXISTS pageinspect;

DROP FUNCTION IF EXISTS gist_node_boxes(text);
CREATE FUNCTION gist_node_boxes(idx text)
RETURNS TABLE(blk int, lvl int, bbox box) LANGUAGE plpgsql AS $$
DECLARE
  cur int[] := ARRAY[0];
  nxt int[];
  l   int := 0;
  b   int;
  c   int;
  isleaf bool;
  maxlvl int;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _node(blk int, lvl int, leaf bool, bbox box)
    ON COMMIT DROP;
  CREATE TEMP TABLE IF NOT EXISTS _edge(parent int, child int) ON COMMIT DROP;
  DELETE FROM _node; DELETE FROM _edge;

  -- обход в ширину: уровни и рёбра
  WHILE coalesce(array_length(cur, 1), 0) > 0 LOOP
    nxt := ARRAY[]::int[];
    FOREACH b IN ARRAY cur LOOP
      SELECT 'leaf' = ANY(o.flags) INTO isleaf
        FROM gist_page_opaque_info(get_raw_page(idx, b)) o;
      INSERT INTO _node(blk, lvl, leaf, bbox) VALUES (b, l, isleaf, NULL);
      IF NOT isleaf THEN
        FOR c IN SELECT (i.ctid::text::point)[0]::int
                   FROM gist_page_items(get_raw_page(idx, b), idx::regclass) i
        LOOP
          INSERT INTO _edge VALUES (b, c);
          nxt := nxt || c;
        END LOOP;
      END IF;
    END LOOP;
    cur := nxt;
    l := l + 1;
  END LOOP;

  -- листья: описывающий прямоугольник точек страницы
  UPDATE _node n SET bbox = s.bx
    FROM (
      SELECT nd.blk,
             box(point(min(pt[0]), min(pt[1])), point(max(pt[0]), max(pt[1]))) AS bx
        FROM _node nd,
             LATERAL (
               SELECT substring(substring(i.keys from 5) from 2
                                for length(substring(i.keys from 5)) - 2)::point AS pt
                 FROM gist_page_items(get_raw_page(idx, nd.blk), idx::regclass) i
                WHERE i.keys IS NOT NULL AND i.keys <> ''
             ) k
       WHERE nd.leaf
       GROUP BY nd.blk
    ) s
   WHERE n.blk = s.blk AND n.leaf;

  -- внутренние узлы снизу вверх: объединение ключей потомков
  SELECT max(_node.lvl) INTO maxlvl FROM _node;
  FOR l IN REVERSE maxlvl - 1 .. 0 LOOP
    UPDATE _node n SET bbox = s.bx
      FROM (
        SELECT e.parent,
               box(point(min((ch.bbox[1])[0]), min((ch.bbox[1])[1])),
                   point(max((ch.bbox[0])[0]), max((ch.bbox[0])[1]))) AS bx
          FROM _edge e JOIN _node ch ON ch.blk = e.child
         GROUP BY e.parent
      ) s
     WHERE n.blk = s.parent AND n.lvl = l;
  END LOOP;

  RETURN QUERY SELECT n.blk, n.lvl, n.bbox FROM _node n ORDER BY n.lvl, n.blk;
END $$;

-- omega_l: среднее число узлов уровня l, ключ которых накрывает случайную точку.
DROP FUNCTION IF EXISTS omega_by_level(text, int);
CREATE FUNCTION omega_by_level(idx text, nprobes int DEFAULT 2000)
RETURNS TABLE(lvl int, nodes bigint, omega numeric, share_of_level numeric)
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
           count(DISTINCT n.blk)::bigint AS nodes,
           round(count(*) FILTER (WHERE q.p <@ n.bbox)::numeric / nprobes, 3) AS omega,
           round(100.0 * count(*) FILTER (WHERE q.p <@ n.bbox)
                 / nprobes / count(DISTINCT n.blk), 4) AS share_of_level
      FROM _nb n CROSS JOIN _q q
     GROUP BY n.lvl
     ORDER BY n.lvl;
END $$;
