/*
    - only factoring in the latest entries from stg_indexing_data model.
        - i.e. where entry_version is 1.
*/

{{
    config(
        materialized='view'
    )
}}

-- referencing stg_indexing_raw_data model
with 
get_stg_indexing_data as (
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
        invoice_url,
        entry_version
    from {{ ref('stg_indexing_data') }}
),

remove_remaining_columns as (
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
    from get_stg_indexing_data
    where entry_version = 1
)

select * from remove_remaining_columns
