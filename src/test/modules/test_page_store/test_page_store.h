/*-------------------------------------------------------------------------
 *
 * test_page_store.h
 *      Test-only contracts, not a storage provider API.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_PAGE_STORE_H
#define TEST_PAGE_STORE_H

#include "access/xlogdefs.h"
#include "lib/stringinfo.h"
#include "storage/relfilelocator.h"
#include "utils/timestamp.h"

#define TEST_PAGE_STORE_MAX_BLOCKS 64
#define TEST_PAGE_SERVICE_COMMAND "TEST_PAGE_SERVICE"
#define TEST_PAGE_SERVICE_VERSION 1
#define TEST_PAGE_SERVICE_MAX_REQUEST 64
#define TEST_PAGE_SERVICE_MAX_RESPONSE (TEST_PAGE_SERVICE_MAX_REQUEST + 9 + TEST_PAGE_STORE_MAX_BLOCKS * BLCKSZ)

extern void test_page_store_protocol_init(void);
extern void test_page_store_worker_init(bool physical_service);
extern bool test_page_store_worker_enabled(void);
extern bool test_page_store_transport_worker;
extern void test_page_store_worker_exchange(StringInfo request, StringInfo response,
											TimestampTz deadline);
extern void test_page_store_client_exchange(StringInfo request, StringInfo response,
											TimestampTz deadline);

extern void test_page_store_check_cut(TimeLineID expected_tli,
									  XLogRecPtr expected_lsn);
extern void test_page_store_history_init(void);
extern bool test_page_store_history_enabled(void);
extern XLogRecPtr test_page_store_history_predecessor(TimeLineID tli, XLogRecPtr start);
extern bytea *test_page_store_history_fetch(RelFileLocator locator,
											ForkNumber forknum, BlockNumber block,
											int count, TimeLineID tli, XLogRecPtr lsn,
											bool wait, bool *exists, BlockNumber *nblocks);

#endif
