/*
    - add a random number column.
    - sort the row in desc based on the above added column.
    - pick-up th requrired number of rows.
*/

{{
    config(
        materialized='view'
    )
}}

with
add_random_number_column as (
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
        indexer_srcf_id,
        random() as random_number
    from {{ ref('stg_required_columns') }}
),

get_sampled_data as (
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
    from add_random_number_column
    where invoice_url is not null
    order by random_number desc
    limit 500
)

select * from get_sampled_data
