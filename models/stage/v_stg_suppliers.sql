{%- set yaml_metadata -%}
source_model: 'raw_suppliers'
derived_columns:
  SUPPLIER_KEY: 'SUPPLIERKEY'
  NATION_KEY: 'SUPPLIER_NATION_KEY'
  RECORD_SOURCE: '!TPCH-ORDERS'
  EFFECTIVE_FROM: 'SUPPLIER_DATE'
hashed_columns:
  SUPPLIER_PK: 'SUPPLIER_KEY'
  LINK_SUPPLIER_NATION_PK:
    - 'SUPPLIER_KEY'
    - 'SUPPLIER_NATION_KEY'
  SUPPLIER_NATION_PK: 'SUPPLIER_NATION_KEY'
  NATION_PK: 'SUPPLIER_NATION_KEY'
  SUPPLIER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'SUPPLIER_KEY'
      - 'SUPPLIER_NAME'
      - 'SUPPLIER_ADDRESS'
      - 'SUPPLIER_PHONE'
      - 'SUPPLIER_ACCBAL'
      - 'SUPPLIER_COMMENT'
  SUPPLIER_NATION_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'SUPPLIER_NATION_NAME'
      - 'SUPPLIER_NATION_COMMENT'
      - 'SUPPLIER_NATION_KEY'

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
