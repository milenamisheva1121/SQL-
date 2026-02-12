create view gold.fact_sales as
select
s.sls_ord_num as order_number,
p.product_key,
c.customer_key,
s.sls_order_dt as order_date,
s.sls_ship_dt as shipping_date,
s.sls_due_dt as due_date,
s.sls_sales as sales_amount,
s.sls_quantity as quantity, 
s.sls_price as price
from  silver.crm_sales_details s	
left join gold.dim_products p
on s.sls_prd_key = p.product_number
left join gold.dim_customers c
on s.sls_cust_id = c.customer_id;


create view gold.dim_customers as 
Select
row_number() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as  customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case
	when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.gen,'n/a') 
	end  as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date


from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
where ci.cst_id is not null;


create view gold.dim_products as
select
row_number() over (order by pr.prd_start_dt, pr.prd_key) as product_key,
	pr.prd_id as product_id,
	pr.prd_key as product_number,
	pr.prd_nm as product_name,
	pr.cat_id as category_id,
	px.cat as category,
	px.subcat as subcategory,
	px.maintenance as maintenance,
	pr.prd_cost as product_cost,
	pr.prd_line as product_line,
	pr.prd_start_dt as product_start_date
from silver.crm_prd_info pr
left join silver.erp_px_cat_g1v2 px
on pr.cat_id = px.id
where pr.prd_end_dt is null;
