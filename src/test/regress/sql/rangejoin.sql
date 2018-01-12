
-- test with unique to exercise more of the planner
create table rangejoin_left(i1 int, ir1 int4range unique);
create table rangejoin_right(i2 int, ir2 int4range unique);

insert into rangejoin_left values
       (1001, int4range(10, 80)),
       (1002, int4range(20, 30)),
       (1003, int4range(21, 25)),
       (1004, int4range(22, 35)),
       (1005, int4range(40, 60)),
       (1006, int4range(50, 60));

insert into rangejoin_right values
       (1000, 'empty'::int4range),
       (1001, int4range(NULL,  4)),
       (1002, int4range(10, 12)),
       (1003, int4range(11, 14)),
       (1004, int4range(20, 45)),
       (1005, int4range(24, 28)),
       (1006, int4range(85, 90));

-- simple inner join
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left, rangejoin_right
  where ir1 && ir2;

select i1, ir1, i2, ir2
  from rangejoin_left, rangejoin_right
  where ir1 && ir2;

-- two predicates
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left inner join rangejoin_right
    on (i1 = i2 and ir1 && ir2);

select i1, ir1, i2, ir2
  from rangejoin_left inner join rangejoin_right
    on (i1 = i2 and ir1 && ir2);

-- left join
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left left join rangejoin_right
    on (ir1 && ir2);

select i1, ir1, i2, ir2
  from rangejoin_left left join rangejoin_right
    on (ir1 && ir2);

-- right join should be implemented as left join
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left right join rangejoin_right
    on (ir1 && ir2);

-- full join doesn't support range join
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left full join rangejoin_right
    on (ir1 && ir2);

-- range input to range join must be ascending
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left inner join rangejoin_right
    on (i1 = i2 and ir1 && ir2)
  order by ir1 desc, i1;

-- but it's OK for non-range inputs to be descending
explain (costs off) select i1, ir1, i2, ir2
  from rangejoin_left inner join rangejoin_right
    on (i1 = i2 and ir1 && ir2)
  order by ir1 nulls first, i1 desc;

select i1, ir1, i2, ir2
  from rangejoin_left inner join rangejoin_right
    on (i1 = i2 and ir1 && ir2)
  order by ir1 nulls first, i1 desc;

drop table rangejoin_left;
drop table rangejoin_right;

create table multirangejoin_left (ir1 int4range, ir2 int4range);
create table multirangejoin_right (ir3 int4range, ir4 int4range);

insert into multirangejoin_left values
  (int4range(30,99), int4range(20,30)),
  (int4range(2,40), int4range(15,27)),
  (int4range(61,99), int4range(20,45)),
  (int4range(22,32), int4range(21,66)),
  (int4range(36,71), int4range(45,49)),
  (int4range(9,80), int4range(2,4));


insert into multirangejoin_right values
  (int4range(9,70), int4range(10,78)),
  (int4range(21,37), int4range(89,99)),
  (int4range(5,98), int4range(35,97)),
  (int4range(12,17), int4range(81,92)),
  (int4range(15,19), int4range(5,55)),
  (int4range(57,58), int4range(42,80));

explain (costs off) select *
  from multirangejoin_left inner join multirangejoin_right
    on (ir1 && ir3 and ir2 && ir4) order by ir1, ir2, ir3, ir4;

select *
  from multirangejoin_left inner join multirangejoin_right
    on (ir1 && ir3 and ir2 && ir4) order by ir1, ir2, ir3, ir4;

set enable_mergejoin=false;
explain (costs off) select *
  from multirangejoin_left inner join multirangejoin_right
    on (ir1 && ir3 and ir2 && ir4) order by ir1, ir2, ir3, ir4;

select *
  from multirangejoin_left inner join multirangejoin_right
    on (ir1 && ir3 and ir2 && ir4) order by ir1, ir2, ir3, ir4;
set enable_mergejoin=true;

explain (costs off) select *
  from multirangejoin_left left join multirangejoin_right
    on (ir1 && ir4 and ir2 && ir3) order by ir4, ir3, ir2, ir1;

select *
  from multirangejoin_left left join multirangejoin_right
    on (ir1 && ir4 and ir2 && ir3) order by ir4, ir3, ir2, ir1;

set enable_mergejoin=false;
explain (costs off) select *
  from multirangejoin_left left join multirangejoin_right
    on (ir1 && ir4 and ir2 && ir3) order by ir4, ir3, ir2, ir1;

select *
  from multirangejoin_left left join multirangejoin_right
    on (ir1 && ir4 and ir2 && ir3) order by ir4, ir3, ir2, ir1;
set enable_mergejoin=true;

drop table multirangejoin_left;
drop table multirangejoin_right;

create table bigrangejoin_left (i1 int, ir1 int4range);
create table bigrangejoin_right (i2 int, ir2 int4range);

-- 100 small ranges
insert into bigrangejoin_left
  select g/4,
	 int4range(g,
		   g + case when (g%2=0) then g%7 else 12-(g%11) end)
    from generate_series(1,100) g;
insert into bigrangejoin_right
  select g/4,
	 int4range(g-7+(g%19),
	           g-7+(g%19) + case when (g%3=0) then g%11 else 17-(g%15) end)
    from generate_series(1,100) g;

-- 10 medium ranges
insert into bigrangejoin_left
  select g/4*10,
	 int4range(g*10,
		   g*10 + case when (g%2=0) then g%7 else 12-(g%11) end)
    from generate_series(1,10) g;
insert into bigrangejoin_right
  select g/4*10,
	 int4range(g*10-57+(g%173),
	           g*10-57+(g%173) + case when (g%3=0) then g%123 else 97-(g%83) end)
    from generate_series(1,10) g;

insert into bigrangejoin_left select g*11-21, 'empty'::int4range
  from generate_series(1,9) g;

insert into bigrangejoin_right select g*13-29, 'empty'::int4range
  from generate_series(1,8) g;

insert into bigrangejoin_left values
  (4, int4range(NULL,5)),
  (93, int4range(95, NULL));

insert into bigrangejoin_right values
  (7, int4range(NULL,3)),
  (92, int4range(99, NULL));

create temp table rangejoin_result1
  (i1 int, ir1 int4range, i2 int, ir2 int4range);
create temp table rangejoin_result2
  (i1 int, ir1 int4range, i2 int, ir2 int4range);

set enable_hashjoin=false;
explain (costs off) insert into rangejoin_result1
  select i1, ir1, i2, ir2
    from bigrangejoin_left left join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by ir1 nulls first, i1 desc;

insert into rangejoin_result1
  select i1, ir1, i2, ir2
    from bigrangejoin_left left join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by ir1 nulls first, i1 desc;
set enable_hashjoin=true;

set enable_mergejoin=false;
explain (costs off) insert into rangejoin_result2
  select i1, ir1, i2, ir2
    from bigrangejoin_left left join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by ir1 nulls first, i1 desc;

insert into rangejoin_result2
  select i1, ir1, i2, ir2
    from bigrangejoin_left left join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by ir1 nulls first, i1 desc;
set enable_mergejoin=true;

select count(*) from rangejoin_result1;
select count(*) from rangejoin_result2;

select * from rangejoin_result1 except select * from rangejoin_result2;

select * from rangejoin_result2 except select * from rangejoin_result1;

drop table rangejoin_result1;
drop table rangejoin_result2;

create temp table rangejoin_result3
  (i1 int, ir1 int4range, i2 int, ir2 int4range);
create temp table rangejoin_result4
  (i1 int, ir1 int4range, i2 int, ir2 int4range);


explain (costs off) insert into rangejoin_result3
  select i1, ir1, i2, ir2
    from bigrangejoin_left inner join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by i1, ir1;

insert into rangejoin_result3
  select i1, ir1, i2, ir2
    from bigrangejoin_left inner join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by i1, ir1;

set enable_mergejoin=false;
explain (costs off) insert into rangejoin_result4
  select i1, ir1, i2, ir2
    from bigrangejoin_left inner join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by i1, ir1;

insert into rangejoin_result4
  select i1, ir1, i2, ir2
    from bigrangejoin_left inner join bigrangejoin_right
      on (i1 = i2 and ir1 && ir2)
        order by i1, ir1;
set enable_mergejoin=true;

select count(*) from rangejoin_result3;
select count(*) from rangejoin_result4;

select * from rangejoin_result3 except select * from rangejoin_result4;

select * from rangejoin_result4 except select * from rangejoin_result3;

drop table rangejoin_result3;
drop table rangejoin_result4;

drop table bigrangejoin_left;
drop table bigrangejoin_right;
