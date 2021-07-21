CREATE OR REPLACE TABLE
  `ygrene.ai_ml.closed_cancel_req_data_details_cleaned`
PARTITION BY
  bq_insert_date AS
SELECT
  SAFE_CAST(AppMonth as INT64) as AppMonth,
  age,
  num_of_improvements,
  Extend_Pay,
  Interest_Rate,
  OrigBidAmt,
  Amort_Period,
  final_approved,
  NTPed,
  Funded,
  FundedAmt,
  Fastpass,
  Max_Contract_Amount,
  dum.*,
  saved,
  CURRENT_DATE() AS bq_insert_date,
  'BQ_User' AS bq_inserted_by,
  CURRENT_DATE() AS bq_update_date,
FROM
  `ygrene.ai_ml.closed_cancel_req_data_details`cl
LEFT JOIN
  ygrene.ai_ml.closed_dancel_deq_data_details_dummies dum
ON
  cl.Project_ID = dum.Project_ID
where age is not null