#-------------------------------------------------------------------------
#
# Makefile for src/interfaces/libpg_shmem library
#
# PostgreSQL Shared Memory Access Library
# External process shared memory access for compute-storage separation
#
# src/interfaces/libpg_shmem/Makefile
#
#-------------------------------------------------------------------------

subdir = src/interfaces/libpg_shmem
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PGFILEDESC = "PostgreSQL Shared Memory Access Library"

# shared library parameters
NAME= pg_shmem
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

# Programs to build and install
PROGRAMS = test_shmem_access

override CPPFLAGS :=  -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/port -I$(top_srcdir)/src/port -I$(top_srcdir)/src/include
ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS = \
	$(WIN32RES) \
	pg_shmem_attach.o \
	pg_shmem_access.o \
	pg_shmem_utils.o

# Add platform-specific objects
ifeq ($(PORTNAME), win32)
OBJS += pg_shmem_win32.o
else
OBJS += pg_shmem_posix.o
endif

# libpg_shmem_pgport is for use by client executables that use libpg_shmem
# Similar to libpq_pgport
ifdef PGXS
libpg_shmem_pgport = -L$(libdir) -lpgcommon_shlib -lpgport_shlib -lpg_shmem
else
libpg_shmem_pgport = -L$(top_builddir)/src/common -lpgcommon_shlib -L$(top_builddir)/src/port -lpgport_shlib -lpg_shmem
endif

SHLIB_LINK_INTERNAL = -L$(top_builddir)/src/port -lpgport_shlib -L$(top_builddir)/src/common -lpgcommon_shlib
SHLIB_PREREQS = submake-libpgport
SHLIB_EXPORTS = exports.txt

all: all-lib all-bin

all-bin: $(PROGRAMS)

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

# We use several port modules verbatim, but since we need to compile with
# appropriate options to build a shared lib, we can't necessarily use the
# same object files as the backend uses. Instead, symlink the source files
# in here and build our own object file.

path.c pgsleep.c pgstrcasecmp.c pqsignal.c \
thread.c strerror.c: % : $(top_srcdir)/src/port/%
	rm -f $@ && $(LN_S) $< .

# depend on Makefile.global to force rebuild on re-run of configure
all-lib:

# Build rules for programs
test_shmem_access: test_shmem_access.o all-lib
	$(CC) $(CFLAGS) test_shmem_access.o -L. -lpg_shmem -L$(top_builddir)/src/common -lpgcommon_shlib -L$(top_builddir)/src/port -lpgport_shlib $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

clean distclean: clean-lib
	rm -f path.c pgsleep.c pgstrcasecmp.c pqsignal.c thread.c strerror.c
	rm -f $(PROGRAMS) test_shmem_access.o
	rm -rf tmp_check

maintainer-clean: distclean maintainer-clean-lib

install: all installdirs install-lib install-bin
	$(INSTALL_DATA) libpg_shmem-fe.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) libpg_shmem.pc '$(DESTDIR)$(pkgconfigdir)/libpg_shmem.pc'

install-bin: $(PROGRAMS) installdirs
	$(INSTALL_PROGRAM) test_shmem_access$(X) '$(DESTDIR)$(bindir)/test_shmem_access$(X)'

installdirs: installdirs-lib
	$(MKDIR_P) '$(DESTDIR)$(includedir)' '$(DESTDIR)$(pkgconfigdir)' '$(DESTDIR)$(bindir)'

uninstall: uninstall-lib uninstall-bin
	rm -f '$(DESTDIR)$(includedir)/libpg_shmem-fe.h'
	rm -f '$(DESTDIR)$(pkgconfigdir)/libpg_shmem.pc'

uninstall-bin:
	rm -f '$(DESTDIR)$(bindir)/test_shmem_access$(X)'

# Add TAP tests
TAP_TESTS = 1

# TAP test targets
check: all-bin
	$(prove_check)

installcheck: all-bin
	$(prove_installcheck)

# Symlinks to backend files
BACKEND_SYMLINKS = pg_shmem.h

$(BACKEND_SYMLINKS): % : $(top_srcdir)/src/include/storage/%
	rm -f $@ && $(LN_S) $< .

ifdef PGXS
distprep:
else
distprep: $(BACKEND_SYMLINKS)
endif 