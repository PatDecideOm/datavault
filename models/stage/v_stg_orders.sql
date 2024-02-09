{%- set yaml_metadata -%}
source_model: 'raw_orders'
derived_columns:
  ORDER_KEY: 'ORDERKEY'
  CUSTOMER_KEY: 'CUSTOMER_KEY'
  RECORD_SOURCE: '!TPCH-ORDERS'
  EFFECTIVE_FROM: 'ORDER_DATE'
hashed_columns:
  ORDER_PK: 'ORDER_KEY'
  ORDER_CUSTOMER_PK:
    - 'ORDER_KEY'
    - 'CUSTOMER_KEY'
  CUSTOMER_PK: 'CUSTOMER_KEY'
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ORDER_KEY'
      - 'CUSTOMER_KEY'
      - 'ORDER_STATUS'
      - 'ORDER_TOTAL_PRICE'
      - 'ORDER_ORDER_DATE'
      - 'ORDER_PRIORITY'
      - 'ORDER_CLERK'
      - 'ORDER_SHIP_PRIORITY'
      - 'ORDER_COMMENT'
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