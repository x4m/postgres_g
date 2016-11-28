delete from box where uid in (select uid from box limit 200000);
