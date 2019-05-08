set datasets_dir = "file:///home/work/data/dataset/user-ip";
set model_dir = "file:///home/work/data/ml/user-ip";

-- 加载数据
load csv.`${datasets_dir}/query_result.csv` options header="true" as data;

select bssid as id, array(bssid, user_ip) as words from data as new_data;

train new_data as word2vec.`${model_dir}/w2v_model` where inputCol="words" and minCount="0" and windowSize="2" and vectorSize="32";

register word2vec.`${model_dir}/w2v_model` as w2v_predict;

--select id, w2v_predict(words[0]) as f0, w2v_predict(words[1]) f1 from new_data as result;

select id, array_concat(w2v_predict_array(words)) as feature from new_data as result;

