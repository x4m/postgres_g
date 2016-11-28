select pg_size_pretty(pg_relation_size('i_box_uid_lids'));
vacuum box;
select pg_size_pretty(pg_relation_size('i_box_uid_lids'));
