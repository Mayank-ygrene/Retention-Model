CREATE OR REPLACE TABLE
  `ygrene.ai_ml.closed_cancel_req_data_details_cleaned_modelling`
PARTITION BY
  bq_insert_date AS(
SELECT
  *,
  CASE
    WHEN split_field < 0.8 THEN 'training'
    WHEN split_field = 0.8 THEN 'evaluation'
    WHEN split_field > 0.8 THEN 'testing'
END
  AS dataframe
FROM (
  SELECT
    *,
    ROUND(ABS(RAND()),1) AS split_field
  FROM
    `ygrene.ai_ml.closed_cancel_req_data_details_cleaned`)
    )