load csv.`/Users/jinchen/test` as t1;
select * from t1 as t2;
save overwrite t2 as csv.`test`;
