# Test deletion of empty GIN posting tree pages during vacuum, running
# concurrently with readers and writers of the same posting tree.
#
# Vacuum deletes empty posting tree leaf pages on the fly during its
# left-to-right sweep of the leaf level, without locking out concurrent
# insertions into the tree (see "Page deletion" in the GIN README).
# This test uses two injection points:
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
# Detach before wakeup: the current waiter is woken up, and the sweep
# does not wait at any of the remaining between-pages points.
step c_detach_wake
{
    SELECT injection_points_detach('gin-vacuum-posting-tree-resume');
    SELECT injection_points_wakeup('gin-vacuum-posting-tree-resume');
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
