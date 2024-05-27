/* contrib/bloom/bloom--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bloom" to load this file. \quit

CREATE FUNCTION blhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bloom TYPE INDEX HANDLER blhandler;
COMMENT ON ACCESS METHOD bloom IS 'bloom index access method';

-- Opclasses

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING bloom AS
	OPERATOR	1	=(int4, int4),
	FUNCTION	1	hashint4(int4);

CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING bloom AS
	OPERATOR	1	=(text, text),
	FUNCTION	1	hashtext(text);


CREATE FUNCTION xthandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD xbtree TYPE INDEX HANDLER xthandler;
COMMENT ON ACCESS METHOD xbtree IS 'like b-tree but for int8';

CREATE OPERATOR CLASS int4_x_ops DEFAULT FOR TYPE int4 USING xbtree AS
		OPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),
		OPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),
		OPERATOR 5 > (int4, int4), FUNCTION 1 btint4cmp(int4,int4);