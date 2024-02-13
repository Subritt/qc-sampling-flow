/*
model to get the random sample data from base_indexing_data model.
*/

{{
    config(
        materialized='view'
    )
 }}

 with get_base_indexing_data as (
    select
        invoice_id,
        indexing_label,
        client_code,
        indexer_srcf_id,
        changed_at,
        invoice_number,
        invoice_date,
        amount,
        company,
        vendor,
        account_number,
        due_date,
        invoice_url
    from {{ ref('base_indexing_data') }}
 ),

 sort_base_indexing_data as (
    select *,
    rank() over (
        partition by invoice_id, indexing_label
        order by changed_at desc
    ) as entry_version
    
    from get_base_indexing_data
 )

 select *
 from sort_base_indexing_data
