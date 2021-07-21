CREATE OR REPLACE TABLE
  `ygrene.ai_ml.closed_cancel_req_data_details`
PARTITION BY
  bq_insert_date AS
SELECT
  project_token AS Project_ID,
  case_owner_full_name,
  cl.state,
  date_time_opened,
  date_time_closed,
  case_origin_detail,
  status_reason,
  project_stage,
  account_name AS Contractor,
  EXTRACT(MONTH
  FROM
    SAFE_CAST(FORMAT_TIMESTAMP( '%Y-%m-%d', PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p', date_time_opened ) )AS DATE)) AS month_opened,
  EXTRACT(MONTH
  FROM
    SAFE_CAST(FORMAT_TIMESTAMP( '%Y-%m-%d', PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p', date_time_closed ) )AS DATE)) AS month_closed,
  EXTRACT(YEAR
  FROM
    SAFE_CAST(FORMAT_TIMESTAMP( '%Y-%m-%d', PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p', date_time_opened ) )AS DATE)) AS year_opened,
  EXTRACT(YEAR
  FROM
    SAFE_CAST(FORMAT_TIMESTAMP( '%Y-%m-%d', PARSE_TIMESTAMP('%m/%d/%Y %I:%M %p', date_time_closed ) )AS DATE)) AS year_closed,
  CASE
    WHEN status_reason = 'Saved' OR status_reason = 'Saved - Non CO Switch' OR status_reason = 'Saved - Retention CO Switch' OR status_reason = 'Accepted Return to Ygrene' THEN 1
  ELSE
  0
END
  AS saved,
  Stage,
  CASE
    WHEN Cond_Approved_ THEN 'YES'
  ELSE
  'NO'
END
  Cond_Approved,
  PropertySearchYrMo,
  AppSubmitYrMo,
  ApprovalStageYrMo,
  NTPYrMo,
  FinalApprovalYrMo,
  FundingYrMo,
  DOB,
  Rate_Plan,
  Program_Name,
  Amort_Period,
  CASE
    WHEN Extend_Pay IS NULL THEN 0
  ELSE
  Extend_Pay
END
  AS Extend_Pay,
  Interest_Rate,
  OrigBidAmt,
  CASE
    WHEN FundedAmt IS NULL THEN ( SELECT AVG(FundedAmt) FROM `ygrene.ai_ml.closed_cancel_req_data`cl LEFT JOIN ygrene.ai_ml.wades_world ww ON cl.project_token = ww.Project_ID WHERE cl.status_reason IN ('Saved', 'Saved - Non CO Switch', 'Saved - Retention CO Switch', 'Accepted Return to Ygrene', 'File to be Cancelled', 'Non-Retention CO Switch', 'Did Not Accept Return to Ygrene'))
  ELSE
  FundedAmt
END
  AS FundedAmt,
  In_Home_Close,
  PO_has_Contractor,
  CASE
    WHEN Fastpass THEN 1
  ELSE
  0
END
  AS Fastpass,
  App_Source,
  Max_Contract_Amount,
  Value_Type,
  CASE
    WHEN New_Improvement IS NULL THEN 'None'
  ELSE
  New_Improvement
END
  AS New_Improvement,
  CASE
    WHEN FinalApprovalYrMo IS NULL THEN 1
  ELSE
  0
END
  AS final_approved,
  CASE
    WHEN NTPYrMo IS NULL THEN 1
  ELSE
  0
END
  AS NTPed,
  CASE
    WHEN FundingYrMo IS NULL THEN 1
  ELSE
  0
END
  AS Funded,
  SUBSTR(AppSubmitYrMo, -2) AS AppMonth,
  CASE
    WHEN New_Improvement IS NULL THEN 0
  ELSE
  ARRAY_LENGTH(SPLIT(New_Improvement, ','))
END
  AS num_of_improvements,
  CASE
    WHEN New_Improvement IS NULL THEN 'None'
  ELSE
  SPLIT(New_Improvement, ',')[
OFFSET
  (0)]
END
  AS primary_improvement,
  ROUND(DATE_DIFF(CURRENT_DATE(), CAST(DOB AS date), DAY)/365.4,0) AS age,
  CURRENT_DATE() AS bq_insert_date,
  'BQ_User' AS bq_inserted_by,
  CURRENT_DATE() AS bq_update_date,
FROM
  `ygrene.ai_ml.closed_cancel_req_data`cl
LEFT JOIN
  ygrene.ai_ml.wades_world ww
ON
  cl.project_token = ww.Project_ID
WHERE
  cl.status_reason IN ('Saved',
    'Saved - Non CO Switch',
    'Saved - Retention CO Switch',
    'Accepted Return to Ygrene',
    'File to be Cancelled',
    'Non-Retention CO Switch',
    'Did Not Accept Return to Ygrene')