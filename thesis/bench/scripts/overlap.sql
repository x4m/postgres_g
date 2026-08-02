-- Измерение перекрытия ключей omega_l по уровням.
--
-- В GiST уровень на странице не хранится (в отличие от B-дерева), поэтому
-- уровни размечаются обходом в ширину от корня (блок 0).
-- Ключ узла уровня l хранится в нисходящей ссылке на странице уровня l-1,
-- поэтому omega_l считается по ключам, лежащим на страницах уровня l-1.

CREATE EXTENSION IF NOT EXISTS pageinspect;

-- Разметка уровней: 0 — корень.
CREATE OR REPLACE FUNCTION gist_levels(idx text)
RETURNS TABLE(blk int, lvl int) LANGUAGE plpgsql AS $$
DECLARE
  cur int[] := ARRAY[0];
  nxt int[];
  l   int := 0;
  b   int;
  c   int;
  isleaf bool;
BEGIN
  WHILE coalesce(array_length(cur, 1), 0) > 0 LOOP
    FOREACH b IN ARRAY cur LOOP
      blk := b; lvl := l; RETURN NEXT;
    END LOOP;
    nxt := ARRAY[]::int[];
    FOREACH b IN ARRAY cur LOOP
      SELECT 'leaf' = ANY(o.flags) INTO isleaf
        FROM gist_page_opaque_info(get_raw_page(idx, b)) o;
      IF NOT isleaf THEN
        FOR c IN
          SELECT (i.ctid::text::point)[0]::int
            FROM gist_page_items(get_raw_page(idx, b), idx::regclass) i
        LOOP
          nxt := nxt || c;
        END LOOP;
      END IF;
    END LOOP;
    cur := nxt;
    l := l + 1;
  END LOOP;
END $$;

-- Все нисходящие ключи с уровнем узла, на который они ссылаются.
CREATE OR REPLACE FUNCTION gist_downlinks(idx text)
RETURNS TABLE(child_lvl int, key box) LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT g.blk, g.lvl FROM gist_levels(idx) g LOOP
    IF NOT (SELECT 'leaf' = ANY(o.flags)
              FROM gist_page_opaque_info(get_raw_page(idx, r.blk)) o) THEN
      -- Ключ печатается как ((x1,y1),(x2,y2)); вырожденный в точку — как ((x,y)).
      RETURN QUERY
        SELECT r.lvl + 1,
               CASE WHEN k LIKE '%),(%'
                    THEN k::box
                    ELSE box(substring(k from 2 for length(k) - 2)::point,
                             substring(k from 2 for length(k) - 2)::point)
               END
          FROM (SELECT substring(i.keys from 5) AS k
                  FROM gist_page_items(get_raw_page(idx, r.blk), idx::regclass) i
                 WHERE i.keys IS NOT NULL AND i.keys <> '') s;
    END IF;
  END LOOP;
END $$;

-- omega_l: среднее число узлов уровня l, ключ которых накрывает случайную точку.
-- Корень (уровень 0) всегда один и всегда посещается, поэтому omega_0 = 1.
CREATE OR REPLACE FUNCTION measure_overlap(idx text, nprobes int DEFAULT 1000)
RETURNS TABLE(lvl int, nodes bigint, omega numeric) LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS _dl(child_lvl int, key box) ON COMMIT DROP;
  DELETE FROM _dl;
  INSERT INTO _dl SELECT * FROM gist_downlinks(idx);
  CREATE TEMP TABLE IF NOT EXISTS _q(p point) ON COMMIT DROP;
  DELETE FROM _q;
  INSERT INTO _q SELECT point(random(), random()) FROM generate_series(1, nprobes);

  -- box имеет оператор = со семантикой равенства площадей, поэтому
  -- считать узлы через DISTINCT нельзя: делим на число зондов.
  RETURN QUERY
    SELECT 0, 1::bigint, 1.0::numeric
    UNION ALL
    SELECT d.child_lvl,
           (count(*) / nprobes)::bigint,
           round(count(*) FILTER (WHERE q.p <@ d.key)::numeric / nprobes, 3)
      FROM _dl d CROSS JOIN _q q
     GROUP BY d.child_lvl
     ORDER BY 1;
END $$;
