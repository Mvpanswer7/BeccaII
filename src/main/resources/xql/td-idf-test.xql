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
-- td-idf中此项被强制为true
and `ignoreNature`="true"

-- 分词的字典路径，支持多个
and `dicPaths`="${datasets_dir}/feature_word_10"

;

register TfIdfInPlace.`${model_dir}/tfidf/` as tfidf_func;

select tfidf_func(features) as k, label from  orginal_text_corpus as test2;
