-- td-idf 预处理

set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/text-classification";
set model_dir = "file:///home/work/data/ml/text-classification";

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


register TfIdfInPlace.`${model_dir}/tfidf/` as tfidf_func;

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


--训练NaiveBeys
train trainingTable as NaiveBayes.`${model_dir}/nb_model`;
register NaiveBayes.`${model_dir}/nb_model` as nb_predict;

-- validate
select vec_argmax(nb_predict(features))  as k, label from validateTable as nb_result;
select count(*) as hit_num from nb_result where k = label as result;


select tfidf_func(features) as k, label from  orginal_text_corpus as test2;

-- 预测
-- load csv.`${datasets_dir}/testing.csv` options header="false" and delimiter="," and quote="'" as test0;
-- select _c0 as id, _c1 as content from test0 as test1;
-- select id, tfidf_func(content) as features from test1 as test2;

--select id, vec_argmax(nb_predict(features)) as k, label from test1 as test3;
--save overwrite test3 json.`file:///home/work/data/test/test3_tfidf`;



