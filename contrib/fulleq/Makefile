MODULE_big = fulleq
OBJS = fulleq.o	
DOCS = README.fulleq
REGRESS = fulleq
DATA = fulleq--2.0.sql fulleq--unpackaged--2.0.sql
EXTENSION=fulleq

ARGTYPE = bool bytea char name int8 int2 int4 text \
	oid xid cid oidvector float4 float8 abstime reltime macaddr \
	inet cidr varchar date time timestamp timestamptz \
	interval timetz

EXTRA_CLEAN = fulleq--2.0.sql fulleq--unpackaged--2.0.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/fulleq
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

#all: fulleq--2.0.sql fulleq--unpackaged--2.0.sql

fulleq--2.0.sql:	fulleq.sql.in
	echo '\echo Use "CREATE EXTENSION fulleq" to load this file. \quit' > $@
	for type in	$(ARGTYPE);	\
	do	\
		sed -e "s/ARGTYPE/$$type/g" < $< >> $@;	\
	done

fulleq--unpackaged--2.0.sql:	fulleq-unpackaged.sql.in
	echo '\echo Use "CREATE EXTENSION fulleq FROM unpackaged" to load this file. \quit' > $@
	echo 'DROP OPERATOR CLASS IF EXISTS int2vector_fill_ops USING hash;' >> $@
	echo 'DROP OPERATOR FAMILY IF EXISTS int2vector_fill_ops USING hash;' >> $@
	echo 'DROP FUNCTION IF EXISTS fullhash_int2vector(int2vector);' >> $@
	echo 'DROP OPERATOR IF EXISTS == (int2vector, int2vector);' >> $@
	echo 'DROP FUNCTION IF EXISTS isfulleq_int2vector(int2vector, int2vector);' >> $@
	for type in	$(ARGTYPE);	\
	do	\
		sed -e "s/ARGTYPE/$$type/g" < $< >> $@;	\
	done

