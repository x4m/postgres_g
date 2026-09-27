# src/test/modules/test_page_store/Makefile

MODULE_big = test_page_store
OBJS = \
	$(WIN32RES) \
	test_page_store.o \
	test_page_history.o \
	test_page_protocol.o \
	test_page_worker.o \
	test_remote_smgr.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)
PGFILEDESC = "test_page_store - replay page service prototype"

EXTENSION = test_page_store
DATA = test_page_store--1.0.sql

TAP_TESTS = 1
EXTRA_INSTALL = contrib/pageinspect contrib/pg_buffercache contrib/amcheck \
	contrib/pg_visibility contrib/pg_freespacemap \
	src/test/modules/injection_points src/test/modules/test_aio

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = src/test/modules/test_page_store
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
