connect tidb where path="insight_sources" as db1;
load tidb.`db1.tidb_news_info` as tidb_news_info ;
select * from tidb_news_info limit 100 as t1;
save overwrite t1 as csv.`jctest_csv` options fileNum="20";
save overwrite t1 as csv.`jctest_csv2` options fileNum="15";