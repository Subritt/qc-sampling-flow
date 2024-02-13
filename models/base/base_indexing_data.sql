/*
Data type, column name transformation
*/

{{ 
    config(
        materialized='table'
    )
 }}

with raw_quality_data as (
    select
       invoice_id,
       indexing_step_2,
       client_code,
       indexer,
       changed_date_est,
       invoice_number,
       invoice_date,
       amount,
       company,
       vendor,
       account_number,
       due_date,
       sr_url1
    from {{ source('quality_data', 'sample_sr_raw_data') }}
),
/*
Column Headers:
---------------
InvoiceID, IndexingStep2, ClientCode, Indexer, ChangedDate_EST,
InvoiceNumber, InvoiceDate, Amount, Company, Vendor, AccountNumber, DueDate,
SR_URL1
*/
/* changed column header */
change_column_header_name as (
    select 
        invoice_id,
        indexing_step_2 as indexing_label,
        client_code,
        indexer as indexer_srcf_id,
        changed_date_est::timestamp as changed_at, -- time in UTC
        invoice_number,
        invoice_date,
        amount,
        company,
        vendor,
        account_number,
        due_date,
        sr_url1 as invoice_url
    from raw_quality_data
),

add_random_number_column as (
    select
        *
    from change_column_header_name
)

select * from add_random_number_column
