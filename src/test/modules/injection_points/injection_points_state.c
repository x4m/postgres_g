/*--------------------------------------------------------------------------
 *
 * injection_points_state.c
 *		Standalone client for the injection point shared state.
 *
 * This small program maps the injection point state file from a data
 * directory (see injection_points.c) and lets a test harness wait for an
 * injection point to be reached and release it, without going through a
 * backend connection.  That is useful when the waiting process has no
 * PGPROC or no wait-event visibility (for example the postmaster, or a
 * process killed mid-flight), where SQL-driven wakeups are not an option.
 *
 * The state is mapped exactly like the backend does -- POSIX mmap() or, on
 * Windows, a file-backed CreateFileMapping() -- because plain file reads are
 * not guaranteed to be coherent with a mapped view on Windows.
 *
 * Usage:
 *	  injection_points_state DATADIR wait   NAME [TIMEOUT_SEC]
 *	  injection_points_state DATADIR wakeup NAME [TIMEOUT_SEC]
 *
 * Exit status: 0 success, 1 usage/IO error, 2 timeout.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points_state.c
 *
 * -------------------------------------------------------------------------
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef WIN32
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

#include "injection_points.h"

#define INJ_DEFAULT_TIMEOUT_SEC		180
#define INJ_POLL_INTERVAL_MS		10

static const char *progname = "injection_points_state";

static void
usage(void)
{
	fprintf(stderr,
			"usage: %s DATADIR {wait|wakeup} NAME [TIMEOUT_SEC]\n",
			progname);
}

/* Sleep for the given number of milliseconds. */
static void
sleep_ms(int ms)
{
#ifndef WIN32
	struct timespec ts;

	ts.tv_sec = ms / 1000;
	ts.tv_nsec = (long) (ms % 1000) * 1000000L;
	nanosleep(&ts, NULL);
#else
	Sleep(ms);
#endif
}

/* Atomically bump a 32-bit counter shared with the backend. */
static void
atomic_inc_u32(volatile uint32_t *counter)
{
#if defined(_MSC_VER)
	_InterlockedIncrement((volatile long *) counter);
#else
	__atomic_add_fetch(counter, 1, __ATOMIC_SEQ_CST);
#endif
}

/*
 * Map the state file under "datadir" using the same primitives as the
 * backend.  Returns the mapped public state, or NULL on failure (with a
 * message printed to stderr).
 */
static InjectionPointPublicState *
map_state(const char *datadir)
{
	char		path[1024];

	snprintf(path, sizeof(path), "%s/%s", datadir, INJ_STATE_FILE);

#ifndef WIN32
	{
		int			fd;
		void	   *base;

		fd = open(path, O_RDWR, 0);
		if (fd < 0)
		{
			fprintf(stderr, "%s: could not open \"%s\": %s\n",
					progname, path, strerror(errno));
			return NULL;
		}

		base = mmap(NULL, sizeof(InjectionPointPublicState),
					PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		close(fd);

		if (base == MAP_FAILED)
		{
			fprintf(stderr, "%s: could not map \"%s\": %s\n",
					progname, path, strerror(errno));
			return NULL;
		}
		return (InjectionPointPublicState *) base;
	}
#else
	{
		HANDLE		hfile;
		HANDLE		hmap;
		void	   *base;

		hfile = CreateFile(path, GENERIC_READ | GENERIC_WRITE,
						   FILE_SHARE_READ | FILE_SHARE_WRITE,
						   NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
		if (hfile == INVALID_HANDLE_VALUE)
		{
			fprintf(stderr, "%s: could not open \"%s\": error code %lu\n",
					progname, path, GetLastError());
			return NULL;
		}

		hmap = CreateFileMapping(hfile, NULL, PAGE_READWRITE, 0,
								 (DWORD) sizeof(InjectionPointPublicState), NULL);
		if (hmap == NULL)
		{
			fprintf(stderr, "%s: could not create mapping for \"%s\": error code %lu\n",
					progname, path, GetLastError());
			CloseHandle(hfile);
			return NULL;
		}

		base = MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0,
							 sizeof(InjectionPointPublicState));
		CloseHandle(hmap);
		CloseHandle(hfile);

		if (base == NULL)
		{
			fprintf(stderr, "%s: could not map \"%s\": error code %lu\n",
					progname, path, GetLastError());
			return NULL;
		}
		return (InjectionPointPublicState *) base;
	}
#endif
}

/*
 * Return the slot index currently registered for "name", or -1 if none.
 *
 * The backend writes the name under a spinlock before it starts waiting, so a
 * stable match means the wait point has been reached.  A torn read simply
 * fails to match and the caller retries.
 */
static int
find_slot(InjectionPointPublicState *state, const char *name)
{
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (strncmp(state->name[i], name, INJ_NAME_MAXLEN) == 0)
			return i;
	}
	return -1;
}

int
main(int argc, char **argv)
{
	const char *datadir;
	const char *mode;
	const char *name;
	int			timeout_sec = INJ_DEFAULT_TIMEOUT_SEC;
	int			max_polls;
	InjectionPointPublicState *state;
	int			slot = -1;

	if (argc < 4 || argc > 5)
	{
		usage();
		return 1;
	}

	datadir = argv[1];
	mode = argv[2];
	name = argv[3];
	if (argc == 5)
		timeout_sec = atoi(argv[4]);

	if (strlen(name) >= INJ_NAME_MAXLEN)
	{
		fprintf(stderr, "%s: injection point name too long\n", progname);
		return 1;
	}
	if (strcmp(mode, "wait") != 0 && strcmp(mode, "wakeup") != 0)
	{
		usage();
		return 1;
	}

	state = map_state(datadir);
	if (state == NULL)
		return 1;

	/*
	 * Poll until the named injection point shows up in a slot, meaning a
	 * process has reached the wait point.  This replaces a fixed sleep and an
	 * unreliable guess that the point was reached.
	 */
	max_polls = (timeout_sec * 1000) / INJ_POLL_INTERVAL_MS;
	for (int polls = 0;; polls++)
	{
		slot = find_slot(state, name);
		if (slot >= 0)
			break;
		if (polls >= max_polls)
		{
			fprintf(stderr, "%s: timed out waiting for injection point \"%s\"\n",
					progname, name);
			return 2;
		}
		sleep_ms(INJ_POLL_INTERVAL_MS);
	}

	/* For "wakeup", release the waiter by bumping its counter. */
	if (strcmp(mode, "wakeup") == 0)
		atomic_inc_u32(&state->wait_counts[slot]);

	return 0;
}
