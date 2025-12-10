# Measles Analytics – AWS + Snowflake + dbt + Snowflake ML

End-to-end modern data stack project combining AWS Lambda, S3, Snowflake, dbt, GitHub Actions CI/CD, and Snowflake ML to analyze measles vaccination coverage vs. measles cases and predict high-risk countries for the following year.

---

## 1. Business Problem

Measles remains a preventable but recurring disease. Countries with gaps in vaccination coverage or rising incidence can experience outbreaks that strain health systems and put vulnerable populations at risk.

This project builds a small but realistic analytics and ML stack to:

- Ingest vaccination coverage and measles cases by country and year.
- Model the relationship between coverage and incidence.
- Predict which countries are at higher risk of measles outbreaks next year.
- Provide a clean mart and risk scores that BI tools can consume.

The focus is on **architecture, governance, automation, and ML integration**, not on perfectly tuned epidemiological models.

---

## 2. High-Level Architecture

**Data flow**

1. **AWS Lambda → S3**

   - Two Lambda functions fetch:
     - Measles vaccination coverage (MCV1 & MCV2).
     - Measles cases by country and year.
   - Lambdas write CSVs into an S3 bucket with date-stamped filenames.

2. **Snowflake External Stage → RAW**

   - A Snowflake **STORAGE INTEGRATION** + AWS IAM role allow Snowflake to read from S3.
   - An external **STAGE** in Snowflake points to the S3 bucket/prefix.
   - A dbt macro (`load_measles_raw_from_s3`) runs `COPY INTO` to load the latest CSVs into RAW tables.

3. **dbt: RAW → STAGE → CORE → MART**

   - dbt models clean and normalize the data:
     - RAW: direct copies of the S3 files.
     - STAGE: type cleaning, column normalization, basic filtering.
     - CORE: dimensional model (e.g., `dim_country`, `fact_measles_cases`, `fact_measles_coverage`).
     - MART: analytics-friendly tables, aggregations and risk features.

4. **Snowflake ML**

   - A classification model (`measles_risk_model`) is trained directly in Snowflake ML using the mart layer.
   - The model predicts whether each country-year will be **high risk** the following year.
   - Predictions are stored and exposed through a clean view for BI tools.

5. **CI/CD with GitHub Actions**
   - CI workflow validates changes on each push/PR:
     - `dbt debug`, `dbt run`, `dbt test` against the dev environment.
   - A separate refresh workflow can orchestrate monthly data refreshes end-to-end.

---

## 3. Technologies

- **Cloud & Compute**

  - AWS Lambda (Python)
  - AWS S3
  - Snowflake (Warehouses, Databases, Roles, Storage Integration, External Stages)

- **Transformation & Modeling**

  - dbt Core
  - Snowflake dbt adapter
  - dbt tests, macros, and environment-specific profiles

- **Machine Learning**

  - Snowflake ML:
    - `CREATE SNOWFLAKE.ML.CLASSIFICATION`
    - `SHOW_EVALUATION_METRICS`
    - `SHOW_FEATURE_IMPORTANCE`
    - `!PREDICT` for inference

- **CI/CD**
  - GitHub Actions
  - Key-pair authentication to Snowflake (no passwords in CI)

---

## 4. Data Model

### 4.1 Layers

- **RAW**

  - `MEASLES_COVERAGE_RAW`
    - Measles vaccination coverage per country/year for MCV1 and MCV2.
    - Single table with an `ANTIGEN` column (MCV1/MCV2).
  - `MEASLES_CASES_RAW`
    - Measles cases per country/year.

- **STAGE**

  - Staging tables that:
    - Standardize column names and data types.
    - Remove unused columns.
    - Handle missing values (e.g., null cases → 0).

- **CORE**

  - `DIM_COUNTRY`: country master data.
  - `FACT_MEASLES_CASES`: measles cases per country/year.
  - `FACT_MEASLES_COVERAGE`: MCV1/MCV2 coverage per country/year.

- **MART**
  - Analytics tables that join cases and coverage and pre-compute risk features.
  - ML-specific mart tables:
    - `MART_MEASLES_RISK_TRAINING`  
      Training dataset for Snowflake ML.
    - `MART_MEASLES_RISK_SCORING`  
      Latest country-year rows used for prediction.
    - `MART_MEASLES_RISK_PREDICTIONS`  
      Raw predictions including the JSON `prediction` object.
    - `MART_MEASLES_RISK_SCORES`  
      Clean view exposing:
      - `predicted_class`
      - `prob_high_risk`
      - `prob_low_risk`

---

## 5. Machine Learning in Snowflake ML

### 5.1 Training dataset

`MART_MEASLES_RISK_TRAINING` contains, for each country-year:

- Features:

  - `MEASLES_CASES` (current year)
  - `TOTAL_CASES_PREV_YEAR`
  - `MEASLES_COVERAGE_MCV1_PCT`, `MEASLES_COVERAGE_MCV2_PCT`
  - `MCV1_PREV_YEAR`, `MCV2_PREV_YEAR`
  - `DELTA_MCV1`, `DELTA_MCV2` (change vs previous year)
  - `YEAR`, `COUNTRY_CODE` (capturing country-specific patterns)

- Target:
  - `HIGH_RISK_NEXT_YEAR` = 1 if next year’s total cases exceed a risk threshold, else 0.

### 5.2 Model training

A Snowflake ML classification model is trained directly on the mart:

```sql
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION measles_risk_model (
  INPUT_DATA     => SYSTEM$REFERENCE(
                      'TABLE',
                      'MEASLES_ANALYTICS_DEV.MART.MART_MEASLES_RISK_TRAINING'
                    ),
  TARGET_COLNAME => 'HIGH_RISK_NEXT_YEAR',
  CONFIG_OBJECT  => { 'on_error': 'ABORT' }
);
```

Snowflake handles data splitting, algorithm selection, training and evaluation internally.

### 5.3 Evaluation and feature importance

- Evaluation metrics:

```sql
CALL measles_risk_model!SHOW_EVALUATION_METRICS();
```

- Typical results (example):

  - **Class 0 (low risk):**
  - Precision ~0.92
  - Recall ~0.94
  - F1 ~0.93
  - Support ~690
  - Class 1 (high risk):
  - Precision ~0.74
  - Recall ~0.65
  - F1 ~0.70
  - Support ~173

  This reflects class imbalance: there are many more low-risk country-years than high-risk ones.

- Feature importance:

  - <small>COUNTRY_CODE
  - MEASLES_CASES
  - YEAR
  - TOTAL_CASES_PREV_YEAR
  - Coverage level and changes (MCV1/MCV2)</small>

  Interpretation: persistent high incidence and drops in coverage are strong predictors of future outbreaks, while there are also strong country-specific patterns.

### 5.4 Prediction and risk scores

Scoring is done on the latest country-year rows in MART_MEASLES_RISK_SCORING:

```sql
CREATE OR REPLACE TABLE MART.MART_MEASLES_RISK_PREDICTIONS AS
SELECT
    s.*,
    measles_risk_model!PREDICT(
        INPUT_DATA => OBJECT_CONSTRUCT(*)
    ) AS prediction
FROM MART.MART_MEASLES_RISK_SCORING A
```

The prediction column is a JSON object (class + probabilities).
A clean view exposes the most relevant fields:

```sql
CREATE OR REPLACE VIEW MART.MART_MEASLES_RISK_SCORES AS
SELECT
    country_code,
    country_name,
    year,
    prediction:"class"::int                  AS predicted_class,
    prediction:"probability":"1"::float      AS prob_high_risk,
    prediction:"probability":"0"::float      AS prob_low_risk
FROM MART.MART_MEASLES_RISK_PREDICTIONS;
```

This view is the main entry point for BI tools (Tableau, etc.) to visualize country-level measles risk.

## 6. CI with GitHub Actions

dbt-ci.yml (simplified description):

- **Triggers:**
- On push to main
- On PRs
- Manual workflow execution

- **Steps:**
- Check out repo
- Setup python
- Reconstruct snowflake private key from GH secrets (key-pair auth)
- Install libraries dbt and snowflake
- Run
  - dbt deps
  - dbt debug
  - deb run
  - debt test

This ensures that every change to the dbt project is validated against the dev Snowflake environment

## 7. Data refresh with GitHub Actions

A second GitHub Actions workflow orchestrates a full data refresh in the dev environment:

1. Lambdas write new CSVs to S3 (scheduled via EventBridge).
2. dbt macro load_measles_raw_from_s3 loads the latest files from the Snowflake external stage into RAW.
3. dbt rebuilds STAGE/CORE/MART and runs tests.
4. (Optionally) the Snowflake ML model is retrained and predictions are regenerated.

This workflow can run on a monthly schedule and/or manually via workflow_dispatch.
A similar pattern can be used for a production environment by switching the dbt target and Snowflake credentials.

## 8. Future improvements:

- Improve the ML model:
- Handle class imbalance with class weights or threshold tuning.
- Add epidemiological features (population, regional clusters, socio-economic indicators).

- Available at PROD:
- Separate dev/prod databases, roles, warehouses and S3 prefixes.
- Add a “deploy to prod” workflow in GitHub Actions that runs dbt and ML scoring against the prod environment.

- Observability:
  - Add cost and performance dashboards for Snowflake usage.
  - Add data quality dashboards on top of dbt tests.

## 9. About de creator:

This code has been created and documented by Roberto Torres. More info at: www.robtorres.tech
