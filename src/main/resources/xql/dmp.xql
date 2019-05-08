set datasets_dir = "/home/work/data/dataset/dmp";
set model_dir = "/home/work/data/ml/dmp";

-- 加载数据
load csv.`${datasets_dir}/data.csv` options header="false" and delimiter="," as origin_data;
select _c1 as id, cast(_c0 as int) as label, vec_dense(array_string_to_int(split(_c2, ":"))) as features from origin_data as data;

-- 数据评估
train data as CorpusExplainInPlace.`${model_dir}/explain` where labelCol="label";
load parquet.`${model_dir}/explain/data` as explain_result;
select * from explain_result as a;

-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train data as RateSampler.`${model_dir}/ratesampler`
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${model_dir}/ratesampler` as data0;

select * from data0 where __split__=1
as validateTable;

select * from data0 where __split__=0
as trainingTable;


-- 校验数据评估
train validateTable as CorpusExplainInPlace.`${model_dir}/explain` where labelCol="label";
load parquet.`${model_dir}/explain/data` as explain_result;
select * from explain_result as validate_table;
save overwrite validate_table as csv.`${model_dir}/result/explain/validate_table`;


--训练GBT
train trainingTable as GBTs.`${model_dir}/gbts_model`;
register GBTs.`${model_dir}/gbts_model` as gbts_predict;
select vec_argmax(gbts_predict(features)) as p_label, label from validateTable as gbts_result;
save overwrite gbts_result as json.`${model_dir}/result/gbts_result`;

/*
--训练RandomForest
train trainingTable as RandomForest.`${model_dir}/rf_model`;
register RandomForest.`${model_dir}/rf_model` as rf_predict;
select vec_argmax(rf_predict(features)) as p_label, label from validateTable as rf_result;
save overwrite rf_result as json.`${model_dir}/result/re_result`;
*/

