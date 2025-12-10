{% macro load_measles_raw_from_s3() %}

  {% set mcv1_stmt %}
    COPY INTO RAW.COVERAGE_RAW
    FROM (
        SELECT
            $1::string                                      AS entity,
            $2::string                                      AS code,
            $3::int                                         AS year,
            'MCV1'                                          as antigen,
            TO_NUMBER(REPLACE($4, ',', ''), 38, 4)         AS coverage_pct
        FROM @RAW.measles_s3_stage/coverage
        (FILE_FORMAT => RAW.ff_measles_csv)
    )
    PATTERN = '.*measles_coverage_mcv1_.*\.csv'
    FORCE = FALSE
  {% endset %}

  {% set mcv2_stmt %}
    COPY INTO RAW.COVERAGE_RAW
    FROM (
        SELECT
            $1::string                                      AS entity,
            $2::string                                      AS code,
            $3::int                                         AS year,
            'MCV2'                                          as antigen,
            TO_NUMBER(REPLACE($4, ',', ''), 38, 4)         AS coverage_pct
        FROM @RAW.measles_s3_stage/coverage
        (FILE_FORMAT => RAW.ff_measles_csv)
    )
    PATTERN = '.*measles_coverage_mcv2_.*\.csv'
    FORCE = FALSE
  {% endset %}

  {% set cases_stmt %}
    COPY INTO RAW.MEASLES_CASES_RAW
    FROM (
        SELECT
            $1::string                                      AS entity,
            $2::string                                      AS code,
            $3::int                                         AS year,
            COALESCE(TO_NUMBER(REPLACE($4, ',', ''), 38, 0),0) AS measles_cases
        FROM @RAW.measles_s3_stage/cases
        (FILE_FORMAT => RAW.ff_measles_csv)
    )
    PATTERN = '.*measles_cases_.*\\.csv'
    FORCE = FALSE
  {% endset %}

  {{ log("Running COPY for MCV1", info=True) }}
  {% do run_query(mcv1_stmt) %}

  {{ log("Running COPY for MCV2", info=True) }}
  {% do run_query(mcv2_stmt) %}

  {{ log("Running COPY for CASES", info=True) }}
  {% do run_query(cases_stmt) %}

{% endmacro %}