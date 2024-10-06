
{{ config(materialized='table') }}

SELECT 
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') as withdraw_time,
    invoice_number as invoice_id,
    to_timestamp(invoice_date, 'YYYY-MM-DD') as invoice_date,
    agreement_number as agreement_id,
    agreement_title as agreement_title,
    original_invoice_amount as original_balance,
    currency as currency,
    country as country,
    backup_report_input as backup_report_input,
    invoice_line_type as invoice_line_type,
    funding_type as funding_type
FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_ovr') }}