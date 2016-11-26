
\setrandom uid 1 1500000
\setrandom lid 1 1000
\setrandom lid2 1 1000
\setrandom lid3 1 1000
BEGIN;
insert into box values(:uid,'{:lid,:lid2,:lid3}');
insert into box values(:uid,'{}');
END;
