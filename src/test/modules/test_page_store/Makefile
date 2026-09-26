# src/test/modules/test_page_store/Makefile

MODULE_big = test_page_store
OBJS = \
	$(WIN32RES) \
	test_page_store.o
PGFILEDESC = "test_page_store - frozen-replay page service prototype"

EXTENSION = test_page_store
DATA = test_page_store--1.0.sql

TAP_TESTS = 1
EXTRA_INSTALL = contrib/pageinspect src/test/modules/injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_page_store
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
