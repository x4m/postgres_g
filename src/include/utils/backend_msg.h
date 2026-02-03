/*--------------------------------------------------------------------
 * backend_msg.h
 *
 * Utility to pass additional message to backend processes.
 * Ex: cancel or terminate messages
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/backend_msg.h
 *
 *--------------------------------------------------------------------
 */

#ifndef BACKEND_MSG_H
#define BACKEND_MSG_H

#define BACKEND_MSG_MAX_LEN 128

extern void BackendMsgShmemInit(void);
extern Size BackendMsgShmemSize(void);
extern void BackendMsgInit(int id);
extern int BackendMsgSet(pid_t pid, const char *msg);
extern int BackendMsgGet(char *buf, int max_len);
extern bool BackendMsgIsSet(void);


#endif /* BACKEND_MSG_H */
