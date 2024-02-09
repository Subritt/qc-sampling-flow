/*
Data type, column name transformation
*/

{{ 
    config(
        materialized='table'
    )
 }}

with raw_quality_data as (
    select *
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
        "InvoiceID" as invoice_id,
        "IndexingStep2" as indexing_label,
        "ClientCode" as client_code,
        "Indexer" as indexer_srcfid,
        "ChangedDate_EST"::timestamp as changed_date_est,
        "InvoiceNumber" as invoice_number,
        "InvoiceDate" as invoice_date,
        "Amount" as amount,
        "Company" as compay,
        "Vendor" as vendor,
        "AccountNumber" as account_number,
        "DueDate" as due_date,
        "SR_URL1" as invoice_url
    from raw_quality_data
),

add_random_number_column as (
    select
        *,
        RANDOM() as random_number
    from raw_quality_data
)

select * from add_random_number_column
