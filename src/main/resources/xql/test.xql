load json.`file:///home/work/data/test/ret_json0` as t0;
select * from t0 limit 100 as t1;
save overwrite t1 as csv.`file:///home/work/data/test/ret_csv0`;