/*
- removing columns that are not required.
- re-arranging the columns as per the requirement.
*/

{{
    config(
        materialized='view'
    )
}}

with
get_stg_latest_entry as (
    select
        invoice_url,
        invoice_date,
        company,
        amount,
        vendor,
        invoice_number,
        account_number,
        due_date,
        client_code,
        invoice_id,
        indexer_srcf_id
    from {{ ref('stg_latest_entry') }}
)

select * from get_stg_latest_entry
