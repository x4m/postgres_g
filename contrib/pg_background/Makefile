# contrib/pg_background/Makefile

MODULE_big = pg_background
OBJS = pg_background.o

EXTENSION = pg_background
DATA = pg_background--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_background
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
