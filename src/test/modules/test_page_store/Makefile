# src/test/modules/test_page_store/Makefile

MODULE_big = test_page_store
OBJS = \
	$(WIN32RES) \
	test_page_store.o \
	test_page_history.o \
	test_page_overlay.o \
	test_wal_durability.o \
	test_wal_store.o \
	test_wal_sender.o \
	test_page_protocol.o \
	test_page_worker.o \
	test_remote_smgr.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)
PGFILEDESC = "test_page_store - replay page service prototype"

EXTENSION = test_page_store
DATA = test_page_store--1.0.sql
EXTRA_CLEAN = test_wal_fetch$(X) test_wal_fetch.o xlogreader_fe.o

TAP_TESTS = 1
export enable_injection_points
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

all: test_wal_fetch$(X)

test_wal_fetch.o: test_page_wire.h

xlogreader_fe.o: $(top_srcdir)/src/backend/access/transam/xlogreader.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DFRONTEND -c $< -o $@

test_wal_fetch$(X): test_wal_fetch.o xlogreader_fe.o
	$(CC) $(CFLAGS) $^ $(libpq_pgport) $(filter-out -lpq,$(LDFLAGS)) $(LDFLAGS_EX) $(LIBS) -o $@

ifndef USE_PGXS
test_wal_fetch$(X): | submake-libpq
endif

.PHONY: install-wal-fetch uninstall-wal-fetch

install: install-wal-fetch

install-wal-fetch: test_wal_fetch$(X)
	$(MKDIR_P) '$(DESTDIR)$(bindir)'
	$(INSTALL_PROGRAM) $< '$(DESTDIR)$(bindir)'

uninstall: uninstall-wal-fetch

uninstall-wal-fetch:
	rm -f '$(DESTDIR)$(bindir)/test_wal_fetch$(X)'
