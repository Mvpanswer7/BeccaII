load kafka.`zyd_test` options `kafka.bootstrap.servers`="192.168.8.151:9092" and `subscribe` = "zyd_test" as stream_table0;
select * from stream_table0 as t0;
save append t0 as json.`file:///home/work/data/test/stream_0` options mode="Append" and duration="10" and checkpointLocation="file:///home/work/data/checkpoint/stream_0";