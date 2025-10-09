# LSN Fix: Why We Use XactLastCommitEnd Instead of XactLastRecEnd

## The Problem

The extension was showing `LSN 0/0` (invalid) instead of the actual commit LSN.

## Root Cause

In `RecordTransactionCommit()` (src/backend/access/transam/xact.c), the flow is:

```c
// Line 1454-1460: Write the commit record to WAL
XactLogCommitRecord(...);
// At this point, XactLastRecEnd is set to the end of the commit record

// Line 1465: Use XactLastRecEnd for replication
replorigin_session_advance(replorigin_session_origin_lsn, XactLastRecEnd);

// Line 1571-1575: Save and reset!
/* remember end of last commit record */
XactLastCommitEnd = XactLastRecEnd;  // ← SAVE IT HERE

/* Reset XactLastRecEnd until the next transaction writes something */
XactLastRecEnd = 0;  // ← RESET TO 0!
```

Then later in `CommitTransaction()`:

```c
// Line 2419-2420: Call our callbacks
CallXactCallbacks(XACT_EVENT_COMMIT);
// At this point, XactLastRecEnd is already 0!
```

## The Solution

PostgreSQL saves the commit LSN to **`XactLastCommitEnd`** before resetting `XactLastRecEnd`.

**Changed:**
```c
// src/backend/access/transam/xact.c, line 3859
item->callback(event, XactLastCommitEnd, item->arg);  // ← Use XactLastCommitEnd
```

**Instead of:**
```c
item->callback(event, XactLastRecEnd, item->arg);  // ← This is 0 by now!
```

## Why Two Variables?

From the code comments in xlog.c (lines 240-244):

> XactLastRecEnd points to end+1 of the last record, and is **reset when we end a top-level transaction**, or start a new one; so it can be used to tell if the current transaction has created any XLOG records.

So:
- **`XactLastRecEnd`**: Tracks current transaction's WAL activity, reset to 0 at transaction end
- **`XactLastCommitEnd`**: Preserved value of the last commit record LSN for callbacks/reference

## Testing

After the fix, dropping a table now shows:

**Before:**
```
INFO:  Dropped table: public.x (OID 16425, relfilenode 16425) at LSN 0/0
```

**After:**
```
INFO:  Transaction commit at LSN 0/15E4A88 dropping 1 table(s)
INFO:  Dropped table: public.x (OID 16425, relfilenode 16425) at LSN 0/15E4A88
```

Success! ✅

