select count(*), count(distinct product_id), count(distinct client_id), count(distinct store_id), 
	count(distinct order_date), min(order_date), max(order_date) from orders;	
	-- 512346	49408	1365	33

select TO_CHAR(order_date , 'Month') as month, count(*) 
	from orders group by TO_CHAR(order_date , 'Month');