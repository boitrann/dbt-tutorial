
{{ config(materialized='table') }}

SELECT 
    to_timestamp(run_date, 'YYYY-MM-DD HH24:MI:SS') withdraw_time,
    inv.invoice_number,
    to_timestamp(inv.invoice_date,'YYYY-MM-DD') invoice_date,
    payment_term, 
    case 
        when due_date <> '' then to_timestamp(due_date, 'YYYY-MM-DD')
        else null
    end due_date, 
    buyer, pub_code, inv.agreement_id, payment_method, transaction_type,
    product_line, po_number, line_number, quantity_ordered, quantity_credited,
    quantity_invoiced, unit_standard_price, unit_selling_price, revenue_amount, sales_order, 
    sales_order_line, description, inv.country, funding_type
FROM {{ source('src_coop', 'y4a_dwc_amz_avc_cop_dtl') }} inv
left join {{ ref('y4a_dwc_amz_cop_ovr') }} ovr 
on ovr.invoice_id = inv.invoice_number and ovr.country = inv.country