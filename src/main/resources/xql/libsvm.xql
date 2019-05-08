set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/libsvm";
set model_dir = "file:///home/work/data/ml/libsvm";

-- 加载数据
load libsvm.`${datasets_dir}/sample_libsvm_data.txt` as data;

-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train data as RateSampler.`${model_dir}/ratesampler`
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${model_dir}/ratesampler` as data0;

select * from data0 where __split__=1
as validateTable;

select * from data0 where __split__=0
as trainingTable;


--训练svm
train trainingTable as LSVM.`${model_dir}/svm_model`;
register LSVM.`${model_dir}/svm_model` as svm_predict;
select svm_predict(features)  as k, label from validateTable as svm_result;
save overwrite svm_result json.`file:///home/work/data/test/svm_ret`;

--训练NaiveBeys
train trainingTable as NaiveBayes.`${model_dir}/nb_model`;
register NaiveBayes.`${model_dir}/nb_model` as nb_predict;
select nb_predict(features)  as k, label from validateTable as nb_result;
save overwrite nb_result json.`file:///home/work/data/test/nb_ret`;

--训练RandomForest
train trainingTable as RandomForest.`${model_dir}/rf_model`;
register RandomForest.`${model_dir}/rf_model` as rf_predict;
select rf_predict(features)  as k, label from validateTable as rf_result;
save overwrite rf_result json.`file:///home/work/data/test/rf_ret`;

