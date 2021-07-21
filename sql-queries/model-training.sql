CREATE OR REPLACE MODEL
  ai_ml.retention_model_split_tv OPTIONS(MODEL_TYPE='BOOSTED_TREE_CLASSIFIER',
    BOOSTER_TYPE = 'GBTREE',
    MAX_TREE_DEPTH=90,
    auto_class_weights = TRUE,
    NUM_PARALLEL_TREE = 300,
    TREE_METHOD = 'HIST',
    DATA_SPLIT_METHOD = 'AUTO_SPLIT',
    input_label_cols=['saved']) AS
SELECT
  * EXCEPT( Project_ID,
    bq_insert_date,
    bq_inserted_by,
    bq_update_date,
    dataframe,
    split_field)
FROM
  `ai_ml.closed_cancel_req_data_details_cleaned_modelling`
WHERE
  dataframe in ('training')