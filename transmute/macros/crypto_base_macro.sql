{% macro crypto_base_macro(tablename) %}

{{
    config(
        materialized='incremental',
        unique_key='uri'
    )
}}

SELECT open_time::timestamp, 
"open"::float, 
high::float, 
low::float, 
"close"::float, 
volume::float, 
close_time::timestamp, 
quote_asset_vol::float, 
trades::int, 
base_asset_vol::float, 
pair, 
"interval",
uuid
FROM source( 'raw_data'  , tablename ) 

{% if is_incremental() %}

WHERE uuid not in (select uuid from {{ this }})

{% endif %}

{{
config({
    "post-hook": [
      "{{ postgres_uindex(this, 'uuid')}}",
    ],
    })
}}

{% endmacro %}