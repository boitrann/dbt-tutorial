
{{ config(materialized='table') }}

SELECT
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') run_date, 
    invoice_number, agreement_number, product_line, vendor,
    vendor_id, method_of_payment, 
    case 
        when effective_date <> '' then to_timestamp(effective_date, 'YYYY-MM-DD')
        else null
    end effective_date, 
    asin, upc,
    price_protected_quantity, pending_po_codes_prior_to_eff_date, description, old_cost, new_cost,
    delta, total_by_asin, invoice_date, funding_type
FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_ppt') }} ptt
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = ptt.invoice_number and ovr.country = ptt.country