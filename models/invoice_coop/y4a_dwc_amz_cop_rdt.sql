
{{ config(materialized='table') }}

SELECT 
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') withdraw_time,
    invoice_number,
    promotion_id,
    promotion_description, 
    coupon_code,
    case 
        when order_day <> '' then to_timestamp(order_day, 'YYYY-MM-DD')
        else null
    end, 
    transaction_id, 
    customer_id, 
    asin, 
    upc,
    quantity, 
    coupon_face_value, 
    redemption_fee, 
    vir.country, invoice_date, funding_type, 
    line_number
FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_vir') }} vir
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = vir.invoice_number and ovr.country = vir.country