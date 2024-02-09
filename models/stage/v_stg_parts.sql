{%- set yaml_metadata -%}
source_model: 'raw_parts'
derived_columns:
  PART_KEY: 'PARTKEY'
  RECORD_SOURCE: '!TPCH-ORDERS'
  EFFECTIVE_FROM: 'PART_DATE'
hashed_columns:
  PART_PK: 'PART_KEY'
  PART_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'PART_KEY'
      - 'PART_NAME'
      - 'PART_MFGR'
      - 'PART_BRAND'
      - 'PART_TYPE'
      - 'PART_SIZE'
      - 'PART_CONTAINER'
      - 'PART_RETAIL_PRICE'
      - 'PART_COMMENT'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
)

SELECT *,
       sysdate() AS LOAD_DATE
FROM staging