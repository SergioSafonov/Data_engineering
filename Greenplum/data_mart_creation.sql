drop TABLE if exists public.dim_client;

CREATE TABLE public.dim_client (
	client_id int4 not NULL,
	client_name varchar(64) not NULL,
	location_area_name varchar(64) NULL
)
WITH (
	orientation=row,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (client_id);

select count(*), count(distinct client_id) from public.dim_client;
-- 1366		1366
select * from public.dim_client limit 100;
-----------------------------------------------

drop TABLE if exists public.dim_product;

CREATE TABLE public.dim_product (
	product_id int4 not NULL,
	product_name varchar(256) not NULL,
	department_name varchar(128) null,
	aisle_name varchar(128) NULL
)
WITH (
	orientation=row,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (product_id);

select count(*), count(distinct product_id) from public.dim_product;
-- 49687	49687
select * from public.dim_product limit 100;
----------------------------------------------

drop TABLE if exists public.dim_store;

CREATE TABLE public.dim_store (
	store_id int4 not NULL,
	store_type_name varchar(64) NULL,
	location_area_name varchar(64) NULL
)
WITH (
	orientation=row,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (store_id);

select count(*), count(distinct store_id) from public.dim_store;
-- 33	33
select * from public.dim_store limit 100;
----------------------------------------------

drop TABLE if exists public.dim_date;

CREATE TABLE public.dim_date (
	date date not NULL,
	day int4 not NULL,
	month int4 not NULL,
	quarter int4 not NULL,
	year int4 not NULL,
	week_day int4 not NULL,
	week int4 not NULL
)
WITH (
	orientation=row,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (date);

select count(*), count(distinct date) from public.dim_date;
-- 120	120
select * from public.dim_date order by date limit 100;
----------------------------------------------

drop TABLE if exists public.fact_order;

CREATE TABLE public.fact_order (
	order_id int4 not NULL,
	order_month int,
	order_date date null,
	store_id int4 NULL,
	client_id int4 NULL,
	product_id int4 NULL,
	quantity int4 NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (order_id);
-- PARTITION BY LIST (order_month);

select count(*), count(distinct order_date) from public.fact_order;
-- 8 112 551	120
select * from public.fact_order limit 100;
----------------------------------------------

drop TABLE if exists public.fact_currency_rate;

CREATE TABLE public.fact_currency_rate (
	currency_base char(3) not NULL,
	currency char(3) not NULL,
	rate_date date not null,
	rate_month int4 not null,
	rate decimal(16,6) not null
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (rate_date,currency);

select count(*), count(distinct currency), count(distinct rate_date) from public.fact_currency_rate;
-- 15	5	3
select * from public.fact_currency_rate order by currency,rate_date;
/*
insert into public.fact_currency_rate (currency_base,currency,rate_date,rate_month,rate)
	select distinct currency_base,currency,rate_date,rate_month,rate from public.fact_currency_rate1

delete from public.fact_currency_rate where rate = 1.054574
*/
----------------------------------------------

drop TABLE if exists public.fact_out_of_stock;

CREATE TABLE public.fact_out_of_stock (
	product_id int4 not NULL,
	process_date date not null
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (product_id, process_date);

/*
insert into public.fact_out_of_stock (product_id, process_date)
	select distinct product_id, process_date from public.fact_out_of_stock1
*/
select count(*), count(distinct process_date), count(distinct product_id) from public.fact_out_of_stock;
-- 3294	2	3246
select count(*), process_date from public.fact_out_of_stock group by process_date order by process_date;

/*
1532	2021-07-05
1762	2021-07-06
1846	2021-07-07
1514	2021-07-08
3224	2021-07-09
1527	2021-07-10
*/