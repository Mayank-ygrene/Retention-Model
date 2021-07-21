SELECT
  *
FROM
  ML.EVALUATE(MODEL `ai_ml.retention_model_split_tv`, 
  (
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
  dataframe = 'testing' 
  ))