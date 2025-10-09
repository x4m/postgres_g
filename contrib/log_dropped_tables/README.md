# log_dropped_tables - PostgreSQL Extension

This PostgreSQL extension demonstrates the extended `XactCallback` functionality that now includes the transaction commit LSN. It logs information about dropped tables along with their commit LSN.

## Features

- Extends `XactCallback` to receive the transaction commit LSN (`XLogRecPtr`)
- Registers a callback that logs dropped table information at commit time
- Reports the commit LSN for each dropped table

## Installation

### Prerequisites

- PostgreSQL with the extended XactCallback implementation
- Meson build system or Make

### Build and Install

Using make:
```bash
cd contrib/log_dropped_tables
make
make install
```

### Configuration

To use this extension, you need to load it when the server starts:

1. Edit `postgresql.conf`:
```
shared_preload_libraries = 'log_dropped_tables'
```

or for session-level loading:
```
session_preload_libraries = 'log_dropped_tables'
```

2. Restart PostgreSQL

## Usage

Once loaded, the extension automatically logs information about dropped tables. No additional SQL commands are needed.

### Example

```sql
CREATE TABLE test_table (id int, name text);
INSERT INTO test_table VALUES (1, 'test');
DROP TABLE test_table;
```

In the PostgreSQL server log, you'll see:
```
INFO:  log_dropped_tables extension loaded
INFO:  Transaction commit at LSN 0/12345678 dropping 1 table(s)
INFO:  Dropped table: public.test_table (OID 16385, relfilenode 16385) at LSN 0/12345678
```

## Implementation Details

This extension demonstrates two key PostgreSQL extension mechanisms:

### 1. Extended XactCallback with LSN

The new XactCallback signature:
```c
typedef void (*XactCallback) (XactEvent event, XLogRecPtr lsn, void *arg);
```

The callback receives:
- `event`: The transaction event type (COMMIT, ABORT, etc.)
- `lsn`: The LSN of the commit record (for commit events)
- `arg`: User-defined callback argument

### 2. Object Access Hooks

To capture table names **before** they're removed from the catalog, this extension uses PostgreSQL's object access hook system:

```c
static void log_dropped_object_access(ObjectAccessType access,
                                     Oid classId, Oid objectId,
                                     int subId, void *arg)
```

**How it works:**
1. **Object Access Hook** (`OAT_DROP` event) fires when a table is being dropped
   - At this point, the catalog entry still exists
   - We capture: table name, schema name, OID, relfilenode
   - Store this info in a transaction-scoped hash table

2. **Transaction Commit Callback** fires at transaction commit
   - Retrieves the stored table info from the hash table
   - Logs each dropped table with its name and commit LSN
   - Cleans up the hash table

This two-phase approach solves the problem that by commit time, catalog entries are already gone.

## License

This extension is distributed under the same license as PostgreSQL.

