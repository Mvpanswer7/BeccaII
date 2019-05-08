-- 设置路径
set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/corpus-sets";
set model_dir = "file:///home/work/data/ml/text-classification/corpus0";

-- 加载数据
load csv.`${datasets_dir}/corpus0.csv` options header="false" and delimiter=","
as lwys_corpus;
select _c0 as id, cast(_c1 as int) as label, _c2 as features from lwys_corpus
as orginal_text_corpus;

-- 数据评估
train orginal_text_corpus as CorpusExplainInPlace.`${model_dir}/explain` where
labelCol="label";
load parquet.`${model_dir}/explain/data` as result;
select * from result;

-- 分词
train orginal_text_corpus as TokenAnalysis.`${model_dir}/token-analysis` where
`dic.paths`=""
and idCol="id"
and inputCol="features"
and filterNatures="n,nr,ad"
and ignoreNature="true"
;

load parquet.`${model_dir}/token-analysis` as tb;

save overwrite tb as json.`${model_dir}/result/token-analysis`;

