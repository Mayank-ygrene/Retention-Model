DECLARE
  Primary_Improvements,
  App_Sources,
  Value_Types,
  case_origin_details,
  project_stages,
  Cond_Approveds,
  In_Home_Closes,
  PO_has_Contractors,
  States,
  Rate_Plans ARRAY<STRING>;
SET
  (Primary_Improvements,
    App_Sources,
    Value_Types,
    case_origin_details,
    project_stages,
    Cond_Approveds,
    In_Home_Closes,
    PO_has_Contractors,
    States,
    Rate_Plans) = (
  SELECT
    AS STRUCT ARRAY_AGG(DISTINCT Primary_Improvement),
    ARRAY_AGG(DISTINCT App_Source),
    ARRAY_AGG(DISTINCT Value_Type),
    ARRAY_AGG(DISTINCT case_origin_detail),
    ARRAY_AGG(DISTINCT project_stage),
    ARRAY_AGG(DISTINCT Cond_Approved),
    ARRAY_AGG(DISTINCT In_Home_Close),
    ARRAY_AGG(DISTINCT PO_has_Contractor),
    ARRAY_AGG(DISTINCT State),
    ARRAY_AGG(DISTINCT Rate_Plan IGNORE NULLS)
  FROM
    `ai_ml.closed_cancel_req_data_details` );
EXECUTE IMMEDIATE
  '''    
CREATE OR REPLACE TABLE `ai_ml.closed_dancel_deq_data_details_dummies` AS
SELECT
  Project_ID, ''' || (
  SELECT
    STRING_AGG("COUNTIF(REGEXP_REPLACE(primary_improvement, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(primary_improvement, r'[^a-zA-Z]', '') || "') AS Primary_Improvement_" || REGEXP_REPLACE(primary_improvement, r'[^a-zA-Z]', '')
    ORDER BY
      Primary_Improvement)
  FROM
    UNNEST(Primary_Improvements) AS Primary_Improvement ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(App_Source, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(App_Source, r'[^a-zA-Z]', '') || "') AS App_Source_" || REGEXP_REPLACE(App_Source, r'[^a-zA-Z]', '')
    ORDER BY
      App_Source)
  FROM
    UNNEST(App_Sources) AS App_Source ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(Value_Type, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(Value_Type, r'[^a-zA-Z]', '') || "') AS Value_Type_" || REGEXP_REPLACE(Value_Type, r'[^a-zA-Z]', '')
    ORDER BY
      Value_Type)
  FROM
    UNNEST(Value_Types) AS Value_Type ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(case_origin_detail, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(case_origin_detail, r'[^a-zA-Z]', '') || "') AS case_origin_detail_" || REGEXP_REPLACE(case_origin_detail, r'[^a-zA-Z]', '')
    ORDER BY
      case_origin_detail)
  FROM
    UNNEST(case_origin_details) AS case_origin_detail ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(project_stage, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(project_stage, r'[^a-zA-Z]', '') || "') AS project_stage_" || REGEXP_REPLACE(project_stage, r'[^a-zA-Z]', '')
    ORDER BY
      project_stage)
  FROM
    UNNEST(project_stages) AS project_stage ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(Cond_Approved, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(Cond_Approved, r'[^a-zA-Z]', '') || "') AS Cond_Approved_" || REGEXP_REPLACE(Cond_Approved, r'[^a-zA-Z]', '')
    ORDER BY
      Cond_Approved)
  FROM
    UNNEST(Cond_Approveds) AS Cond_Approved ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(In_Home_Close, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(In_Home_Close, r'[^a-zA-Z]', '') || "') AS In_Home_Close_" || REGEXP_REPLACE(In_Home_Close, r'[^a-zA-Z]', '')
    ORDER BY
      In_Home_Close)
  FROM
    UNNEST(In_Home_Closes) AS In_Home_Close ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(PO_has_Contractor, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(PO_has_Contractor, r'[^a-zA-Z]', '') || "') AS PO_has_Contractor_" || REGEXP_REPLACE(PO_has_Contractor, r'[^a-zA-Z]', '')
    ORDER BY
      PO_has_Contractor)
  FROM
    UNNEST(PO_has_Contractors) AS PO_has_Contractor ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(State, r'[^a-zA-Z]', '') = '" || REGEXP_REPLACE(State, r'[^a-zA-Z]', '') || "') AS State_" || REGEXP_REPLACE(State, r'[^a-zA-Z]', '')
    ORDER BY
      State)
  FROM
    UNNEST(States) AS State ) || (
  SELECT
    ', ' || STRING_AGG("COUNTIF(REGEXP_REPLACE(Rate_Plan, r'[^a-zA-Z0-9]', '') = '" || REGEXP_REPLACE(Rate_Plan, r'[^a-zA-Z0-9]', '') || "') AS Rate_Plan_" || REGEXP_REPLACE(Rate_Plan, r'[^a-zA-Z0-9]', '')
    ORDER BY
      Rate_Plan)
  FROM
    UNNEST(Rate_Plans) AS Rate_Plan ) || '''
FROM
  `ai_ml.closed_cancel_req_data_details`
GROUP BY
  Project_ID
ORDER BY
  Project_ID
''';
SELECT
  *
FROM
  ai_ml.closed_dancel_deq_data_details_dummies;