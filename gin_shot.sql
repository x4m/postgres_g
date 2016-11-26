--\setrandom uid 1 1500000
\set uid 777
--\setrandom lid 1 1000
--\setrandom lid2 1 1000
--\setrandom lid3 1 1000
\set lid 1
\set lid2 2
\set lid3 3
BEGIN;
insert into box values(:uid,'{:lid,:lid2,:lid3}');
insert into box values(:uid,'{}');
END;
