select version();

/*
drop TABLE if exists public.payment;

CREATE TABLE public.payment (
	payment_id integer NULL,
	customer_id integer NULL,
	staff_id integer NULL,
	rental_id integer NULL,
	amount real NULL,
	payment_date varchar(29) NULL
)
DISTRIBUTED RANDOMLY;

-- import data	(export data from PostgreSQL table payment - 16 049 rows)
*/
insert into public.payment 
	select payment_id+payment_id as payment_id,customer_id,staff_id,rental_id,amount,payment_date 
		from public.payment;

select count(*), count(distinct payment_id), count(distinct customer_id), count(distinct staff_id), 
		count(distinct rental_id), min(payment_id), max(payment_id), min(payment_date), max(payment_date) 
	from public.payment p;		-- 65736704	208637	599	2	16044	16050	131473408	2020-01-24 23:21:56.996577+02	2020-05-14 15:44:29.996577+03

select * from public.payment p;
--------------------------------------------------------------------------
/*
drop TABLE if exists public.payment2;

CREATE TABLE public.payment2 (
	payment_id integer NULL,
	customer_id integer NULL,
	staff_id integer NULL,
	rental_id integer NULL,
	amount real NULL,
	payment_date varchar(29) NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (payment_id);

insert into public.payment2 select * from public.payment;
*/
select count(*)	from public.payment2 p;		-- 65 736 704
select * from public.payment2 p;
--------------------------------------------------------------------------
/*
drop TABLE if exists public.payment3;

CREATE TABLE public.payment3 (
	payment_id integer NULL,
	customer_id integer NULL,
	staff_id integer NULL,
	rental_id integer NULL,
	amount real NULL,
	payment_date varchar(29) NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (customer_id);

insert into public.payment3 select * from public.payment;
*/
select count(*)	from public.payment3 p;		-- 65 736 704
select * from public.payment3 p;
----------------------------------------------------------------------------------------
/*
drop TABLE if exists public.payment4;

CREATE TABLE public.payment4 (
	payment_id integer NULL,
	customer_id integer NULL,
	staff_id integer NULL,
	rental_id integer NULL,
	amount real NULL,
	payment_date varchar(29) NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (staff_id);

insert into public.payment4 select * from public.payment;
*/
select count(*)	from public.payment4 p;		-- 65 736 704
select * from public.payment4 p;
----------------------------------------------------------------------------------------

select payment_id,count(*)	from public.payment group by payment_id;		-- DISTRIBUTED RANDOMLY			39,77 s first 200
select payment_id,count(*)	from public.payment2 group by payment_id;		-- DISTRIBUTED BY (payment_id)	27,53 s first 200
select payment_id,count(*)	from public.payment3 group by payment_id;		-- DISTRIBUTED BY (customer_id)	27,70 s first 200
select payment_id,count(*)	from public.payment4 group by payment_id;		-- DISTRIBUTED BY (staff_id)	27,68 s first 200

select customer_id,count(*) from public.payment group by customer_id;		-- 599	DISTRIBUTED RANDOMLY			23,45 s first 200
select customer_id,count(*) from public.payment2 group by customer_id;		-- 599	DISTRIBUTED BY (payment_id)		11,62 s first 200
select customer_id,count(*) from public.payment3 group by customer_id;		-- 599	DISTRIBUTED BY (customer_id)	11,19 s first 200
select customer_id,count(*) from public.payment4 group by customer_id;		-- 599	DISTRIBUTED BY (staff_id)		11,54 s first 200

select staff_id,count(*) from public.payment group by staff_id;				-- 2	DISTRIBUTED RANDOMLY			22,26 s
select staff_id,count(*) from public.payment2 group by staff_id;			-- 2	DISTRIBUTED BY (payment_id)		9,75 s
select staff_id,count(*) from public.payment3 group by staff_id;			-- 2	DISTRIBUTED BY (customer_id)	9,54 s
select staff_id,count(*) from public.payment4 group by staff_id;			-- 2	DISTRIBUTED BY (staff_id)		9,36 s
-------------------------------------------------------------------------------------------

drop TABLE public.clients;

CREATE TABLE public.clients (
	client_id serial4 NOT NULL,
	fullname varchar(127) NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (client_id);

select count(*), count(distinct client_id) from public.clients;				-- 1365	1365

--------------------------------------------------------------------------------------------------

select * from films;