# contrib/log_dropped_tables/Makefile

MODULE_big = log_dropped_tables
OBJS = \
	$(WIN32RES) \
	log_dropped_tables.o

EXTENSION = log_dropped_tables
DATA = log_dropped_tables--1.0.sql

PGFILEDESC = "log_dropped_tables - log dropped tables with commit LSN"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/log_dropped_tables
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

