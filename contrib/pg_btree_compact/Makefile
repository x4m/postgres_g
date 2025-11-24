# contrib/pg_btree_compact/Makefile

MODULE_big = pg_btree_compact
OBJS = \
	$(WIN32RES) \
	pg_btree_compact.o

EXTENSION = pg_btree_compact
DATA = pg_btree_compact--1.0.sql
PGFILEDESC = "pg_btree_compact - compact bloated B-tree indexes"

REGRESS = btree_compact

# Need amcheck and pageinspect for testing
EXTRA_INSTALL = contrib/amcheck contrib/pageinspect

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_btree_compact
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

