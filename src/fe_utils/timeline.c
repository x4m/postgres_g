/*-------------------------------------------------------------------------
 *
 * timeline.c
 *	  Frontend support for timeline history files.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/fe_utils/timeline.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/logging.h"
#include "common/pg_parse_lsn.h"
#include "fe_utils/timeline.h"

/*
 * Parse a timeline history file.
 *
 * The result is a malloc'd array in chronological order, ending with an
 * entry for targetTLI.  The input buffer is modified in place.
 */
TimeLineHistoryEntry *
parseTimeLineHistory(char *buffer, TimeLineID targetTLI, int *nentries)
{
	char	   *fline;
	TimeLineHistoryEntry *entry;
	TimeLineHistoryEntry *entries = NULL;
	int			nlines = 0;
	TimeLineID	lasttli = 0;
	XLogRecPtr	prevend;
	char	   *bufptr;
	bool		lastline = false;

	prevend = InvalidXLogRecPtr;
	bufptr = buffer;
	while (!lastline)
	{
		char	   *ptr;
		char	   *token_end;
		char		save;
		TimeLineID	tli;
		XLogRecPtr	switchpoint;
		bool		valid_switchpoint = false;
		int			nchars;
		size_t		nspaces;

		fline = bufptr;
		while (*bufptr && *bufptr != '\n')
			bufptr++;
		if (!(*bufptr))
			lastline = true;
		else
			*bufptr++ = '\0';

		/* Skip leading whitespace and check for a comment. */
		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		if (sscanf(fline, "%u%n", &tli, &nchars) != 1)
		{
			pg_log_error("syntax error in history file: %s", fline);
			pg_log_error_detail("Expected a numeric timeline ID.");
			exit(1);
		}

		ptr = fline + nchars;
		nspaces = strspn(ptr, " \t\n\r\f\v");
		if (nspaces > 0)
		{
			ptr += nspaces;
			token_end = ptr + strcspn(ptr, " \t\n\r\f\v");
			save = *token_end;
			*token_end = '\0';
			valid_switchpoint = pg_parse_lsn(ptr, &switchpoint);
			*token_end = save;
		}

		if (!valid_switchpoint)
		{
			pg_log_error("syntax error in history file: %s", fline);
			pg_log_error_detail("Expected a write-ahead log switchpoint location.");
			exit(1);
		}
		if (entries && tli <= lasttli)
		{
			pg_log_error("invalid data in history file: %s", fline);
			pg_log_error_detail("Timeline IDs must be in increasing sequence.");
			exit(1);
		}

		lasttli = tli;
		nlines++;
		entries = pg_realloc_array(entries, TimeLineHistoryEntry, nlines);
		entry = &entries[nlines - 1];
		entry->tli = tli;
		entry->begin = prevend;
		entry->end = switchpoint;
		prevend = entry->end;
	}

	if (entries && targetTLI <= lasttli)
	{
		pg_log_error("invalid data in history file");
		pg_log_error_detail("Timeline IDs must be less than child timeline's ID.");
		exit(1);
	}

	nlines++;
	if (entries)
		entries = pg_realloc_array(entries, TimeLineHistoryEntry, nlines);
	else
		entries = pg_malloc_array(TimeLineHistoryEntry, 1);

	entry = &entries[nlines - 1];
	entry->tli = targetTLI;
	entry->begin = prevend;
	entry->end = InvalidXLogRecPtr;

	*nentries = nlines;
	return entries;
}
