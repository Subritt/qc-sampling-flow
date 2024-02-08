{{ 
    config(
        materialized='table'
    )
 }}

with
raw_quality_data as (
    select *
    from {{ source('quality_data', 'sample_sr_raw_data') }}
),

select * from raw_quality_data
