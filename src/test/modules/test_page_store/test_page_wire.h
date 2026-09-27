/*-------------------------------------------------------------------------
 *
 * test_page_wire.h
 *      Test-only wire constants shared by frontend and backend consumers.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_PAGE_WIRE_H
#define TEST_PAGE_WIRE_H

#define TEST_PAGE_STORE_MAX_BLOCKS 64
#define TEST_PAGE_SERVICE_COMMAND "TEST_PAGE_SERVICE"
#define TEST_PAGE_SERVICE_VERSION 1
#define TEST_PAGE_SERVICE_MAX_REQUEST 64
#define TEST_PAGE_SERVICE_MAX_RESPONSE (TEST_PAGE_SERVICE_MAX_REQUEST + 9 + TEST_PAGE_STORE_MAX_BLOCKS * BLCKSZ)
#define TEST_WAL_SERVICE_COMMAND "TEST_WAL_SERVICE"
#define TEST_WAL_STORE_MAX_BYTES (64 * 1024)
#define TEST_WAL_STORE_MAX_HISTORY (64 * 1024)
#define TEST_WAL_SERVICE_MAX_REQUEST (33 + Max(TEST_WAL_STORE_MAX_BYTES, TEST_WAL_STORE_MAX_HISTORY))

#endif
