# contrib/heapcheck/Makefile

MODULE_big = heapcheck
OBJS = \
	$(WIN32RES) \
	heapcheck.o

EXTENSION = heapcheck
DATA = heapcheck--1.0.sql
PGFILEDESC = "heapcheck - page corruption information"

REGRESS = 001_create_extension 002_disallowed_reltypes

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/heapcheck
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
