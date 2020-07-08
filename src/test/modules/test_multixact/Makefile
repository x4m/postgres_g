# src/test/modules/test_multixact/Makefile

MODULE_big = test_multixact
OBJS = \
	$(WIN32RES) \
	test_multixact.o
PGFILEDESC = "test_multixact - stress-test for multixact subsystem"

EXTENSION = test_multixact
DATA = test_multixact--1.0.sql

REGRESS = test_multixact

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_multixact
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
