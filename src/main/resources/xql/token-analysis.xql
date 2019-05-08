-- 待处理文本
set datasets_dir = "file:///home/work/app/gitavlyun/insight-xmatrix/src/main/resources/datasets/text-classification";

-- 加载数据
load csv.`${datasets_dir}/training.csv` options header="false" and delimiter="," and quote="'"
as lwys_corpus;

select genUUID() as id, _c1 as features,cast(_c0 as int) as label from lwys_corpus
as orginal_text_corpus;


train orginal_text_corpus TokenAnalysis.`${datasets_dir}/model` where
and idCol="id"
and inputCol="features";

load parquet.`${datasets_dir}/model` as tb;

select * from tb