# Test deletion of empty GIN posting tree pages during vacuum, running
# concurrently with readers and writers of the same posting tree.
#
# Vacuum deletes empty posting tree leaf pages on the fly during its
# left-to-right sweep of the leaf level, without locking out concurrent
# insertions into the tree (see "Page deletion" in the GIN README).
# This test uses three injection points:
#
# - gin-vacuum-posting-tree-resume fires between two leaf pages of the
#   sweep, while no buffer locks or pins are held, so it can be used as
#   a 'wait' point to pause vacuum in the middle of a posting tree.
#
# - gin-vacuum-delete-posting-page fires when an empty leaf page is
#   about to be deleted, with the leaf, its left sibling and its parent
#   all exclusively locked.  A 'wait' here pauses vacuum at its maximum
#   lock footprint.
#
# - gin-finish-incomplete-split fires when a backend is about to finish
#   an incompletely split page, holding the split page exclusively
#   locked and about to lock its parent.
#
# The last two permutations are deadlock provocations: they suspend one
# side of the vacuum-vs-insert lock dance at its maximum lock footprint
# and let the other side run into those locks.  The page deletion
# protocol acquires locks leaf-to-parent and left-to-right on both
# sides, so the blocked side must always get unstuck once the suspended
# side is resumed.  If the protocol regressed and allowed a lock cycle,
# these permutations would hang and time out.
#
# The expected notice counts assume the default 8KB BLCKSZ: the posting
# tree built by the setup has 8 pages, of which 3 become empty and
# deletable after the DELETE.
#
# All rows carry the single array element 1, so the index has one entry
# tree key whose TIDs form one posting tree.  fastupdate is disabled to
# keep the pending list out of the picture.

setup
{
    CREATE EXTENSION injection_points;
    CREATE EXTENSION amcheck;
    CREATE TABLE gin_pt(k serial, i int4[]) WITH (autovacuum_enabled = off);
    CREATE INDEX gin_pt_idx ON gin_pt USING gin(i) WITH (fastupdate = off);
    INSERT INTO gin_pt(i) SELECT array[1] FROM generate_series(1, 30000);
}

teardown
{
    DROP TABLE gin_pt;
    DROP EXTENSION amcheck;
    DROP EXTENSION injection_points;
}

session vacuumer
setup
{
    SELECT injection_points_set_local();
}
step v_attach_notice
{
    SELECT injection_points_attach('gin-vacuum-delete-posting-page', 'notice');
}
step v_attach_wait
{
    SELECT injection_points_attach('gin-vacuum-posting-tree-resume', 'wait');
}
step v_attach_wait_delete
{
    SELECT injection_points_attach('gin-vacuum-delete-posting-page', 'wait');
}
step v_delete
{
    DELETE FROM gin_pt WHERE k BETWEEN 5000 AND 25000;
}
step v_vacuum
{
    VACUUM gin_pt;
}
step v_detach_notice
{
    SELECT injection_points_detach('gin-vacuum-delete-posting-page');
}
# Empty step: launching it waits for this session's VACUUM to complete,
# which lets later steps of other sessions depend on that completion.
step v_noop
{
}

session inserter
setup
{
    SELECT injection_points_set_local();
}
step i_attach_error
{
    SELECT injection_points_attach('gin-leave-leaf-split-incomplete', 'error');
}
# The batch is large enough to certainly split the rightmost leaf of
# the posting tree; the injected error then aborts the insertion,
# leaving the split incomplete (no downlink for the new right half).
step i_split_fail
{
    DO $$
    BEGIN
        INSERT INTO gin_pt(i) SELECT array[1] FROM generate_series(1, 20000);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'insert failed: %', SQLERRM;
    END;
    $$;
}
step i_detach_error
{
    SELECT injection_points_detach('gin-leave-leaf-split-incomplete');
}
step i_attach_wait_finish
{
    SELECT injection_points_attach('gin-finish-incomplete-split', 'wait');
}
# Descends onto the incompletely split leaf and pauses just before
# finishing the split, holding an exclusive lock on that leaf.
step i_insert_one
{
    INSERT INTO gin_pt(i) VALUES (array[1]);
}
step i_insert_batch
{
    INSERT INTO gin_pt(i) SELECT array[1] FROM generate_series(1, 1000);
}
# Empty step: launching it waits for this session's insert to complete,
# which lets later steps of other sessions depend on that completion.
step i_noop
{
}

session checker
setup
{
    SET enable_seqscan = off;
}
step c_count
{
    SELECT count(*) FROM gin_pt WHERE i @> array[1];
}
step c_insert
{
    INSERT INTO gin_pt(i) SELECT array[1] FROM generate_series(1, 1000);
}
# Detach before wakeup: the current waiter is woken up, and the point
# does not make anyone wait again.
step c_detach_wake
{
    SELECT injection_points_detach('gin-vacuum-posting-tree-resume');
    SELECT injection_points_wakeup('gin-vacuum-posting-tree-resume');
}
# The polling loop makes the provocation deterministic: vacuum is only
# released after the concurrent insertion is confirmed to be blocked
# behind one of the buffer locks vacuum holds.
step c_wait_insert_blocked_then_wake_delete
{
    DO $$
    BEGIN
        WHILE NOT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE query LIKE '%INSERT INTO gin_pt%'
              AND pid != pg_backend_pid()
              AND wait_event_type = 'Buffer')
        LOOP
            PERFORM pg_sleep(0.001);
        END LOOP;
    END;
    $$;
    SELECT injection_points_detach('gin-vacuum-delete-posting-page');
    SELECT injection_points_wakeup('gin-vacuum-delete-posting-page');
}
# This must be a single step: while vacuum is blocked on a buffer lock,
# the isolationtester cannot detect it as waiting, so it would never
# launch a further step.  Everything needed to unwind the lock chain
# has to happen within one step.  The polling loop in the middle makes
# the provocation deterministic: the inserter is only released after
# vacuum is confirmed to be blocked behind the leaf the inserter holds.
step c_wake_resume_then_finish
{
    SELECT injection_points_detach('gin-vacuum-posting-tree-resume');
    SELECT injection_points_wakeup('gin-vacuum-posting-tree-resume');
    DO $$
    BEGIN
        WHILE NOT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE query LIKE '%VACUUM gin_pt%'
              AND pid != pg_backend_pid()
              AND wait_event_type = 'Buffer')
        LOOP
            PERFORM pg_sleep(0.001);
        END LOOP;
    END;
    $$;
    SELECT injection_points_detach('gin-finish-incomplete-split');
    SELECT injection_points_wakeup('gin-finish-incomplete-split');
}
step c_check
{
    SELECT gin_index_check('gin_pt_idx');
}

# Baseline: vacuum deletes the emptied middle pages of the posting tree
# (3 notices), searches keep working, and the index structure is sound.
permutation v_attach_notice v_delete v_vacuum v_detach_notice c_count c_check

# Pause vacuum between two leaf pages of the sweep, before it has
# deleted anything.  While vacuum sleeps inside the posting tree, a
# concurrent session can both search the tree and insert into it: with
# the pre-v19 protocol the insert (and, during the deletion pass, also
# the search) would have blocked behind vacuum's cleanup lock on the
# posting tree root.  Then release vacuum and let it finish deleting
# the empty pages (3 notices).
permutation v_attach_notice v_attach_wait v_delete v_vacuum c_count c_insert c_detach_wake v_detach_notice c_count c_check

# Deadlock provocation, vacuum side suspended: pause vacuum just before
# it deletes the first empty leaf, holding exclusive locks on the leaf,
# its left sibling, and the parent (here the posting tree root).  A
# concurrent insertion then blocks on the root.  The tester cannot
# detect a buffer lock wait, hence the (*) marker; vacuum is released
# only once the insertion is observed blocked, and the completion
# report order is pinned to vacuum first.  Once vacuum is released, it
# deletes all 3 pages and the insertion goes through.
permutation v_attach_wait_delete v_delete v_vacuum i_insert_batch(*, v_vacuum) c_wait_insert_blocked_then_wake_delete i_noop c_count c_check

# Deadlock provocation, inserter side suspended: manufacture an
# incompletely split leaf, then pause a backend that is about to finish
# that split, while it holds the leaf exclusively locked.  Vacuum is
# paused at its first between-pages point, then released; its sweep
# runs into the leaf held by the suspended inserter and blocks on it,
# with vacuum's own lock footprint (coupled leaf pair) already in
# place.  Releasing the inserter lets it lock the parent, insert the
# downlink and finish, which unblocks vacuum.
permutation i_attach_error i_split_fail i_detach_error i_attach_wait_finish i_insert_one v_attach_wait v_vacuum(i_insert_one) c_wake_resume_then_finish v_noop c_count c_check
