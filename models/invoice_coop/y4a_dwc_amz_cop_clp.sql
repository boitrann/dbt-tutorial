
{{ config(materialized='table') }}

SELECT 
    'CLF' ref_type, 
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') withdraw_time, 
    invoice_number, 
    promotion_id, 
    promotion_description,
    case 
        when clip_day <> '' then to_timestamp(clip_day, 'YYYY-MM-DD')
        else null
    end, 
    clipped_coupons, 
    clip_source, 
    clip_fee, 
    total_clip_fee,
    clp.country, invoice_date, funding_type

FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_clp') }} clp
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = clp.invoice_number and ovr.country = clp.country