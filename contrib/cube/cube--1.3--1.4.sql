/* contrib/cube/cube--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION cube UPDATE TO '1.4'" to load this file. \quit

UPDATE pg_opclass SET opcdefault = FALSE WHERE opcname = 'gist_cube_ops';

CREATE OPERATOR CLASS gist_cube_ops_v2
    DEFAULT FOR TYPE cube USING gist AS
	OPERATOR	3	&& ,
	OPERATOR	6	= ,
	OPERATOR	7	@> ,
	OPERATOR	8	<@ ,
	OPERATOR	13	@ ,
	OPERATOR	14	~ ,
	OPERATOR	15	~> (cube, int) FOR ORDER BY float_ops,
	OPERATOR	16	<#> (cube, cube) FOR ORDER BY float_ops,
	OPERATOR	17	<-> (cube, cube) FOR ORDER BY float_ops,
	OPERATOR	18	<=> (cube, cube) FOR ORDER BY float_ops,

	FUNCTION	1	g_cube_consistent (internal, cube, smallint, oid, internal),
	FUNCTION	2	g_cube_union (internal, internal),
	FUNCTION	5	g_cube_penalty (internal, internal, internal),
	FUNCTION	6	g_cube_picksplit (internal, internal),
	FUNCTION	7	g_cube_same (cube, cube, internal),
	FUNCTION	8	g_cube_distance (internal, cube, smallint, oid, internal);