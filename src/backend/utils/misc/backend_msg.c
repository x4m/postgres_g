/*--------------------------------------------------------------------
 * backend_msg.h
 *
 * Utility to pass additional message to backend processes.
 * Ex: cancel or terminate messages
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/misc/backend_msg.c
 *
 *--------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/ipc.h"
#include "utils/backend_msg.h"

typedef struct {
	pid_t pid;
	slock_t lock;
	char msg[BACKEND_MSG_MAX_LEN];
} BackendMsgSlot;


static BackendMsgSlot *BackendMsgSlots;
static BackendMsgSlot *MyBackendMsgSlot;

static void
backend_msg_slot_clean(int code, Datum arg)
{
	(void) code;
	(void) arg;

	Assert(MyBackendMsgSlot != NULL);

	SpinLockAcquire(&MyBackendMsgSlot->lock);

	MyBackendMsgSlot->msg[0] = '\0';
	MyBackendMsgSlot->pid = 0;

	SpinLockRelease(&MyBackendMsgSlot->lock);

	MyBackendMsgSlot = NULL;
}


void BackendMsgShmemInit(void)
{
	Size	size;
	bool	found;

	size = BackendMsgShmemSize();
	BackendMsgSlots = ShmemInitStruct("BackendMsgSlots", size, &found);

	if (found)
		return;
	
	memset(BackendMsgSlots, 0, size);

	for (int i = 0; i < MaxBackends; ++i)
		SpinLockInit(&BackendMsgSlots[i].lock);
}

Size
BackendMsgShmemSize(void)
{
	return mul_size(MaxBackends, sizeof(BackendMsgSlot));
}

void BackendMsgInit(int id)
{
	BackendMsgSlot		*slot;

	slot = &BackendMsgSlots[id];

	slot->msg[0] = '\0';
	slot->pid = MyProcPid;

	MyBackendMsgSlot = slot;

	on_shmem_exit(backend_msg_slot_clean, Int32GetDatum(0) /* not used */);
}

int BackendMsgSet(pid_t pid, const char *msg)
{
	BackendMsgSlot		*slot;
	int					len;

	if (msg == NULL || msg[0] == '\0')
		return 0;

	for (int i = 0; i < MaxBackends; ++i)
	{
		slot = &BackendMsgSlots[i];

		if (slot->pid == 0 || slot->pid != pid)
			continue;

		SpinLockAcquire(&slot->lock);

		if (slot->pid != pid)
		{
			SpinLockRelease(&slot->lock);
			break;
		}

		len = stpncpy(slot->msg, msg, sizeof(slot->msg) - 1) - slot->msg;
		slot->msg[len] = '\0';

		SpinLockRelease(&slot->lock);

		return len;
	}

	ereport(LOG,
			(errmsg("Can't set message for missing backend %ld, requested by %ld",
				(long) pid, (long) MyProcPid)));

	return -1;
}

int BackendMsgGet(char *buf, int max_len)
{
	int		len;

	if (MyBackendMsgSlot == NULL)
		return 0;

	SpinLockAcquire(&MyBackendMsgSlot->lock);

	len = strlcpy(buf, MyBackendMsgSlot->msg, max_len);
	memset(MyBackendMsgSlot->msg, '\0', sizeof(MyBackendMsgSlot->msg));

	SpinLockRelease(&MyBackendMsgSlot->lock);

	return len;
}

bool BackendMsgIsSet(void)
{
	bool result = false;

	if (MyBackendMsgSlot == NULL)
		return false;

	SpinLockAcquire(&MyBackendMsgSlot->lock);
	result = MyBackendMsgSlot->msg[0] != '\0';
	SpinLockRelease(&MyBackendMsgSlot->lock);

	return result;
}
