
{{ config(materialized='table') }}

SELECT 
    case when rebate is null then 'PMC_1' else 'PMC_2' end ref_type,
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') withdraw_time, 
    invoice_number, 
    case 
        when order_date <> '' then to_timestamp(order_date, 'YYYY-MM-DD')
        else null
    end order_date, 
    case 
        when ship_date <> '' then to_timestamp(ship_date, 'YYYY-MM-DD')
        else null
    end ship_date, 
    case 
        when return_date <> '' then to_timestamp(return_date, 'YYYY-MM-DD')
        else null
    end return_date, 
    case 
        when cost_date <> '' then to_timestamp(cost_date, 'YYYY-MM-DD')
        else null
    end cost_date, 
    transaction_type, quantity, net_sales, net_sales_currency,
    list_price, list_price_currency, 
    case when rebate is not null then rebate else rebate_in_agreement_currency::numeric end promotion_cost,
    case when rebate_currency is not null then rebate_currency else agreement_currency end cost_currency,
    asin, upc, ean, manufacturer, distributor, product_group, category, subcategory, title,
    binding, promotion_id, cost_type, order_country, acr.country, invoice_date, funding_type,
    line_number
FROM 
    {{ source('src_coop', 'y4a_dwc_amz_avc_cop_acr') }} acr
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = acr.invoice_number and ovr.country = acr.country
WHERE
    (coalesce(order_date,'') <> '' or coalesce(ship_date,'') <> '')