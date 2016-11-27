select pg_size_pretty(pg_relation_size('box'));
vacuum box;
select pg_size_pretty(pg_relation_size('box'));
