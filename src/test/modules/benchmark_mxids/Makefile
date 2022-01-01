# src/test/modules/benchmark_mxids/Makefile

MODULE_big = benchmark_mxids
OBJS = \
	$(WIN32RES) \
	benchmark_mxids.o
PGFILEDESC = "benchmark_mxids - test code to bench multixacts"

EXTENSION = benchmark_mxids
DATA = benchmark_mxids--1.0.sql

REGRESS = benchmark_mxids

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/benchmark_mxids
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
