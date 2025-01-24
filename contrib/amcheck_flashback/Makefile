# contrib/amcheck_flashback/Makefile

MODULE_big	= amcheck_flashback
OBJS = \
	$(WIN32RES) \
	verify_heapam.o \
	verify_nbtree.o

EXTENSION = amcheck_flashback
DATA = amcheck_flashback--1.2--1.3.sql amcheck_flashback--1.1--1.2.sql amcheck_flashback--1.0--1.1.sql amcheck_flashback--1.0.sql
PGFILEDESC = "amcheck_flashback - function for verifying relation integrity"

REGRESS = check check_btree check_heap

EXTRA_INSTALL = contrib/pg_walinspect
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/amcheck_flashback
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
