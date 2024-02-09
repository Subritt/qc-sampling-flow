/*
model to get the random sample data from base_indexing_data model.
*/

{{ 
    config(
        materialized='view'
    )
 }}

 with get_base_indexing_data as (
    select *
    from {{ ref('base_indexing_data') }}
 ),

 sort_base_indexing_data as (
    select *
    from get_base_indexing_data
    order by random_number desc
 )

 select * from sort_base_indexing_data
