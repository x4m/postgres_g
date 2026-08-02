-- Класс операторов, отличающийся от штатного point_ops ровно одной опорной
-- функцией: порядок обхода пространства. Всё остальное — consistent, union,
-- penalty, picksplit, same, distance, fetch — берётся то же самое.
-- Это и есть демонстрация тезиса: смена кривой требует одной функции.
--
-- Номера стратегий и сигнатуры взяты из каталога:
--   select amopstrategy, oprname, amoplefttype::regtype, amoprighttype::regtype
--     from pg_amop join pg_operator on oid=amopopr
--     join pg_opfamily f on f.oid=amopfamily where f.opfname='point_ops';

CREATE FUNCTION gist_point_hilbert_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OPERATOR CLASS point_hilbert_ops
FOR TYPE point USING gist AS
    OPERATOR 1  <<  (point, point),
    OPERATOR 5  >>  (point, point),
    OPERATOR 6  ~=  (point, point),
    OPERATOR 10 <<| (point, point),
    OPERATOR 11 |>> (point, point),
    OPERATOR 15 <-> (point, point) FOR ORDER BY float_ops,
    OPERATOR 28 <@  (point, box),
    OPERATOR 29 <^  (point, point),
    OPERATOR 30 >^  (point, point),
    OPERATOR 48 <@  (point, polygon),
    OPERATOR 68 <@  (point, circle),
    FUNCTION 1  gist_point_consistent(internal, point, smallint, oid, internal),
    FUNCTION 2  gist_box_union(internal, internal),
    FUNCTION 3  gist_point_compress(internal),
    FUNCTION 5  gist_box_penalty(internal, internal, internal),
    FUNCTION 6  gist_box_picksplit(internal, internal),
    FUNCTION 7  gist_box_same(box, box, internal),
    FUNCTION 8  gist_point_distance(internal, point, smallint, oid, internal),
    FUNCTION 9  gist_point_fetch(internal),
    FUNCTION 11 gist_point_hilbert_sortsupport(internal),
    STORAGE box;
