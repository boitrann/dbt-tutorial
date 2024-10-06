
{{ config(materialized='table') }}

SELECT
    run_date, invoice_number , agreement_number , start_date , end_date , 
    description , asin , product_name , discount , currency , country 
FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_agm') }} ptt