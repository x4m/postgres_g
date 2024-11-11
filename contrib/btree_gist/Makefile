# contrib/btree_gist/Makefile

MODULE_big = btree_gist

OBJS =  \
	$(WIN32RES) \
	btree_bit.o \
	btree_bool.o \
	btree_bytea.o \
	btree_cash.o \
	btree_date.o \
	btree_enum.o \
	btree_float4.o \
	btree_float8.o \
	btree_gist.o \
	btree_inet.o \
	btree_int2.o \
	btree_int4.o \
	btree_int8.o \
	btree_interval.o \
	btree_macaddr.o \
	btree_macaddr8.o \
	btree_numeric.o \
	btree_oid.o \
	btree_text.o \
	btree_time.o \
	btree_ts.o \
	btree_utils_num.o \
	btree_utils_var.o \
	btree_uuid.o

EXTENSION = btree_gist
DATA = btree_gist--1.0--1.1.sql \
       btree_gist--1.1--1.2.sql btree_gist--1.2.sql btree_gist--1.2--1.3.sql \
       btree_gist--1.3--1.4.sql btree_gist--1.4--1.5.sql \
       btree_gist--1.5--1.6.sql btree_gist--1.6--1.7.sql \
       btree_gist--1.7--1.8.sql btree_gist--1.8--1.9.sql
PGFILEDESC = "btree_gist - B-tree equivalent GiST operator classes"

REGRESS = init int2 int2_buffering int4 int4_buffering int8 int8_buffering float4 \
		  float4_buffering float8 float8__buffering cash cash_buffering \
 		  oid oid_buffering timestamp timestamp_buffering timestamptz \
 		  timestamptz_buffering time time_buffering timetz timetz_buffering date \
 		  date_buffering interval interval_buffering macaddr macaddr_buffering \
 		  macaddr8 macaddr8_buffering inet inet_buffering cidr cidr_buffering text \
 		  text_buffering varchar varchar_buffering char char_buffering \
          bytea bytea_buffering bit bit_buffering varbit varbit_buffering numeric \
          numeric_buffering uuid uuid_buffering not_equal not_equal_buffering \
          enum enum_buffering bool bool_buffering partitions partitions_buffering \
          stratnum without_overlaps cleanup

SHLIB_LINK += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/btree_gist
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
