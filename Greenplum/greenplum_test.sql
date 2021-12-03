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

insert into public.payment select payment_id+payment_id as payment_id,customer_id,staff_id,rental_id,amount real,payment_date from public.payment;
*/

select count(*), count(distinct payment_id), count(distinct customer_id), count(distinct staff_id), count(distinct rental_id), min(payment_id), max(payment_id) 
	from public.payment p;		-- 65 736 704	208637	599	2	16044	16050	131473408

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
DISTRIBUTED BY (staff_id);

insert into public.payment3 select * from public.payment;
*/
select count(*)	from public.payment3 p;		-- 65 736 704
select * from public.payment3 p;
----------------------------------------------------------------------------------------

select payment_id,count(*)	from public.payment group by payment_id;		-- DISTRIBUTED RANDOMLY			69	(85) s
select payment_id,count(*)	from public.payment2 group by payment_id;		-- DISTRIBUTED BY (payment_id)	7,805	(11,731) s
select payment_id,count(*)	from public.payment3 group by payment_id;		-- DISTRIBUTED BY (staff_id)	8,155	(13,318) s

select staff_id,count(*) from public.payment group by staff_id;				-- 2	DISTRIBUTED RANDOMLY			34,251	(38,715) s
select staff_id,count(*) from public.payment2 group by staff_id;			-- 2	DISTRIBUTED BY (payment_id)		4,677	(4,341) s
select staff_id,count(*) from public.payment3 group by staff_id;			-- 2	DISTRIBUTED BY (staff_id)		4,265	(4,130) s

