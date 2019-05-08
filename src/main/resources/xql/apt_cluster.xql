set datasets_dir = "file:///home/work/data/dataset/apt";
set model_dir = "file:///home/work/data/ml/apt";

-- 加载数据
load csv.`${datasets_dir}/apt_cluster.csv` options header="true" as data;

select md5 as id, 'copyright' as class, copyright as value from data where copyright is not null
union
select md5 as id, 'description' as class, description as value from data where description is not null
union
select md5 as id, 'file_version' as class, file_version as value from data where file_version is not null
union
select md5 as id, 'internal_name' as class, internal_name as value from data where internal_name is not null
union
select md5 as id, 'original_name' as class, original_name as value from data where original_name is not null
union
select md5 as id, 'product' as class, product as value from data where product is not null
union
select md5 as id, 'path' as class, path as value from data where path is not null
union
select md5 as id, 'md5_s' as class, md5_s as value from data where md5_s is not null
union
select md5 as id, 'domain' as class, domain as value from data where domain is not null
union
select md5 as id, 'imphash' as class, imphash as value from data where imphash is not null
union
select md5 as id, 'ip' as class, ip as value from data where ip is not null
union
select md5 as id, 'companyname' as class, companyname as value from data where companyname is not null
union
select md5 as id, 'avl_tags' as class, avl_tags as value from data where avl_tags is not null
union
select md5 as id, 'des' as class, des as value from data where des is not null
union
select md5 as id, 'tag' as class, tag as value from data where tag is not null
union
select md5 as id, 'result' as class, result as value from data where result is not null
union
select md5 as id, 'category' as class, category as value from data where category is not null
as t1;
select id, class, v from t1 lateral view explode(split(value, ',')) v_table as v where v != "" as t2;

select id, class, trim(v) as v from t2 as t3;

train t3 as StringIndex.`${model_dir}/si` where inputCol="id";
register StringIndex.`${model_dir}/si` as p_si;

select class, v, collect_set(id) as ids from t3 group by class, v as t4;

--select count(*) from t4 where size(ids) > 3 as t5

train t4 as FPGrowth.`${model_dir}/fp-growth` where minSupport="0.002" and minConfidence="0.002" and itemsCol="ids";
register FPGrowth.`${model_dir}/fp-growth` as predict;

load parquet.`${model_dir}/fp-growth/data` as tt0;
select * from tt0;

--select size(predict(ids)) as p from t4 as t5;
--select count(*) from t5 where p > 0 as t6


-- select count(*) from t2 where class = 'domain' as t3;







