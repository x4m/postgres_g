/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages and SLRU pages.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/block.h"

/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);

extern uint16 pg_checksum_slru_page(char *page);

extern uint16 pg_getchecksum_slru_page(char *page);

extern void pg_setchecksum_slru_page(char *page);

/* Size of checksum in bytes default 2 bytes (uint16) */
#define CHKSUMSZ 2

#endif							/* CHECKSUM_H */
