set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets";
set model_dir = "file:///home/work/data/ml/car";

-- 加载数据
load csv.`${datasets_dir}/car.csv` as data options header="false" and delimiter=",";

select _c0 as buying, _c1 as maint, _c2 as doors, _c3 as persons, _c4 as lug_boot, _c5 as safety, _c6 as label from data
as origin_data;

train origin_data as CorpusExplainInPlace.`${model_dir}/explain` where
labelCol="label";

load parquet.`${model_dir}/explain/data` as result;
select * from result;

-- 把属性为字符串变换为数字
train origin_data StringIndex.`${model_dir}/si_buying`
where inputCol="buying";
register StringIndex.`${model_dir}/si_buying` as predict_buying;

train origin_data StringIndex.`${model_dir}/si_maint`
where inputCol="maint";
register StringIndex.`${model_dir}/si_maint` as predict_maint;

train origin_data StringIndex.`${model_dir}/si_lug_boot`
where inputCol="lug_boot"
register StringIndex.`${model_dir}/si_lug_boot` as predict_lug_boot;

train origin_data StringIndex.`${model_dir}/si_safety`
where inputCol="safety"
register StringIndex.`${model_dir}/si_safety` as predict_safety;

train origin_data StringIndex.`${model_dir}/si_label`
where inputCol="label"
register StringIndex.`${model_dir}/si_label" as predict_label;

-- 变换
select predict_buying(buying) as buying, predict_maint(maint) as maint, doors, persons, predict_lug_boot(lug_boot) as lug_boot, predict_label(label) as label from origin_data as tmp_data;
select vec_dense2sparse(vec_dense(array_string_to_double(array(buying, maint, doors, persons, lug_boot)))) as features, label from tmp_data as final_data;



