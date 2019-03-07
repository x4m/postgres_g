# src/test/modules/test_blockset/Makefile

MODULE_big = test_blockset
OBJS = test_blockset.o $(WIN32RES)
PGFILEDESC = "test_blockset - test code for block set library"

EXTENSION = test_blockset
DATA = test_blockset--1.0.sql

REGRESS = test_blockset

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_blockset
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
