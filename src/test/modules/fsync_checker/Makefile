# src/test/modules/fsync_checker/Makefile

MODULE_big = fsync_checker
OBJS = $(WIN32RES) fsync_checker_smgr.o
PGFILEDESC = "fsync_checker - SMGR extension for checking volatile relations"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/fsync_checker
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
