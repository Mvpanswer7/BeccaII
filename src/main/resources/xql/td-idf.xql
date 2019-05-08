-- td-idf 预处理

set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/text-classification";
set model_dir = "file:///home/work/data/ml";

-- 加载数据
load csv.`${datasets_dir}/training.csv` options header="false" and delimiter="," and quote="'"
as lwys_corpus;

select _c1 as features,cast(_c0 as int) as label from lwys_corpus
as orginal_text_corpus;

train orginal_text_corpus as TfIdfInPlace.`${model_dir}/tfidf`
where inputCol="features"

-- 分词的字典路径，支持多个
and `dicPaths`="${datasets_dir}/feature_word_10"

;

load parquet.`${model_dir}/tfidf/data`
as lwys_corpus_with_featurize;

-- 把label转化为递增数字
train lwys_corpus_with_featurize StringIndex.`${model_dir}/si`
where inputCol="label";

register StringIndex.`${model_dir}/si` as predict;

select predict(label) as label,features as features from lwys_corpus_with_featurize
as lwys_corpus_final_format;

-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train lwys_corpus_final_format as RateSampler.`${model_dir}/ratesampler`
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${model_dir}/ratesampler` as data2;

select * from data2 where __split__=1
as validateTable;

select * from data2 where __split__=0
as trainingTable;


--训练SKLearn贝叶斯模型
train trainingTable as SKLearn.`${model_dir}/model`
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="tf-idf"
and `kafkaParam.group_id`="g-1"
and `kafkaParam.reuse`="false"
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="11"
and  `fitParam.0.alg`="MultinomialNB"
and `systemParam.pythonPath`="/home/work/miniconda3/envs/xmatrix/bin/python"
and `systemParam.pythonVer`="3.6"
;

-- 注册模型
register SKLearn.`${model_dir}/model` as nb_predict;

select nb_predict(features)  as k, label from validateTable as predict_result;

save overwrite predict_result json.`file:///home/work/data/test/predict_ret0`;


