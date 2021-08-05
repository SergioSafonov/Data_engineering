select version();

/*
drop TABLE if exists public.payment;

CREATE TABLE public.payment (
	payment_id serial NOT NULL,
	customer_id int4 NOT NULL,
	staff_id int4 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamptz NOT NULL
);
-- export data from PostgreSQL table payment - 16 049 rows

insert into public.payment2 select * from public.payment;
*/

select count(*), count(distinct payment_id), count(distinct customer_id), count(distinct staff_id), count(distinct rental_id) 
	from public.payment p;		-- 8217088	16049	599	2	16044	(16 049)

select * from public.payment p;
--------------------------------------------------------------------------
/*
drop TABLE if exists public.payment2;

CREATE TABLE public.payment2 (
	payment_id serial NOT NULL,
	customer_id int4 NOT NULL,
	staff_id int4 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamptz NOT NULL
)
WITH (
	orientation=column,
	compresstype=zlib,
	compresslevel=7,
	appendonly=true
)
DISTRIBUTED BY (customer_id);

insert into public.payment2 select * from public.payment;
*/
select count(*)	from public.payment2 p;		-- 8 217 088
select * from public.payment2 p;
--------------------------------------------------------------------------
/*
drop TABLE if exists public.payment3;

CREATE TABLE public.payment3 (
	payment_id serial NOT NULL,
	customer_id int4 NOT NULL,
	staff_id int4 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamptz NOT NULL
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
select count(*)	from public.payment3 p;		-- 8217088
select * from public.payment3 p;
----------------------------------------------------------------------------------------

select customer_id,count(*)	from public.payment group by customer_id;		-- 5,126 s
select customer_id,count(*)	from public.payment2 group by customer_id;		-- 0,798 s
select customer_id,count(*)	from public.payment3 group by customer_id;		-- 1,158 s	(0,586 s!)

select staff_id,count(*) from public.payment group by staff_id;				-- 2,126 s
select staff_id,count(*) from public.payment2 group by staff_id;			-- 0,671 s	(0,785 s)
select staff_id,count(*) from public.payment3 group by staff_id;			-- 0,703 s	(1,226 s)

