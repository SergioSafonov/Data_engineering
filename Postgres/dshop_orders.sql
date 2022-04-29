select count(*) from aisles a ;				-- 134
select count(*) from clients c ;			-- 1366
select count(*) from departments d ;		-- 21
select count(*) from location_areas la ;	-- 8
select count(*) from products p ;			-- 49687
select count(*) from store_types st ;		-- 4
select count(*) from stores s ;				-- 66

select count(*), count(distinct product_id), count(distinct client_id), count(distinct store_id), 
	count(distinct order_date), min(order_date), max(order_date) from orders;	
	-- 16 225 780	49687	1366	33	120	2021-01-01	2021-04-30

select TO_CHAR(order_date , 'Month') as month, count(*) 
	from orders group by TO_CHAR(order_date , 'Month');
/*
April    	3636512
February 	3268602
January  	4638250
March    	4682416
*/