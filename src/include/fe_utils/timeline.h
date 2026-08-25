/*-------------------------------------------------------------------------
 *
 * timeline.h
 *	  Frontend support for timeline history files.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/fe_utils/timeline.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FE_UTILS_TIMELINE_H
#define FE_UTILS_TIMELINE_H

#include "access/timeline.h"

extern TimeLineHistoryEntry *parseTimeLineHistory(char *buffer,
												  TimeLineID targetTLI,
												  int *nentries);

#endif							/* FE_UTILS_TIMELINE_H */
