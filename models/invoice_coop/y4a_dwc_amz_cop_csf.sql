
{{ config(materialized='table') }}

SELECT 
    case 
        when rebate is not null then 'CSF_1'
        when rebate_in_agreement_currency is not null then 'CSF_2'
        else 'CSF_3' end ref_type,
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') withdraw_time, 
    invoice_number,
    case 
        when receive_date <> '' then to_timestamp(receive_date, 'YYYY-MM-DD')
        else null
    end receive_date,
    case 
        when return_date <> '' then to_timestamp(return_date, 'YYYY-MM-DD')
        else null
    end return_date, 
    case 
        when invoice_day <> '' then to_timestamp(invoice_day, 'YYYY-MM-DD')
        else null
    end invoice_day, 
    transaction_type, 
    case when revised_invoice_quantity is not null then revised_invoice_quantity else quantity end quantity, 
    net_receipts, net_receipts_currency,
    list_price, list_price_currency, 
    case 
        when rebate is not null then rebate
        when rebate_in_agreement_currency is not null then rebate_in_agreement_currency::numeric
        else revised_invoice_rebate end csf_cost,
    case 
        when rebate is not null then rebate_currency
        when rebate_in_agreement_currency is not null then agreement_currency
        else rebate_currency end csfcost_currency,       
    case 
        when rebate_in_purchase_order_currency = '' then null else rebate_in_purchase_order_currency::numeric end rebate_in_purchase_order_curre, 
    purchase_order_currency, new_quantity, new_rebate,
    old_invoice_quantity, old_invoice_rebate, purchase_order, 
    asin, upc, ean, manufacturer, distributor, product_group, category, subcategory, title,
    binding, cost_currency, old_cost, new_cost, price_protection_agreement, 
    case 
        when price_protection_day <> '' then to_timestamp(price_protection_day, 'YYYY-MM-DD')
        else null
    end price_protection_day, 
    cost_variance, invoice, acr.country, funding_type, invoice_date, line_number
FROM 
    {{ source('src_coop', 'y4a_dwc_amz_avc_cop_acr') }} acr
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = acr.invoice_number and ovr.country = acr.country
WHERE
    (coalesce(receive_date,'') <> '' or coalesce(return_date,'') <> '')