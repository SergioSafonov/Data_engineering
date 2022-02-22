-- 1. 
select
	cat.name as category_name,
	count(*) as films_count
from
	category cat
inner join
	film_category fc 
	on fc.category_id = cat.category_id
group by 
	cat.name
order by 
	films_count desc;

-- 2. 
select 
	a.first_name,
	a.last_name,
	sum(f.rental_duration) as rental_duration 
from 
	actor a 
inner join
	film_actor fa 
	on fa.actor_id = a.actor_id
inner join
	film f 
	on f.film_id = fa.film_id
group by 
	a.first_name,
	a.last_name
order by 
	rental_duration desc
limit 10;

-- 3. 
with category_costs
as
(
	select
		cat.name as category_name,
		sum(f.replacement_cost) as replacement_cost
	from
		category cat
	inner join
		film_category fc 
		on fc.category_id = cat.category_id
	inner join
		film f
		on f.film_id = fc.film_id
	group by 
		cat.name
)
select 
	category_name,
	replacement_cost
from 
	category_costs
where
	replacement_cost = 
		(select max(replacement_cost) from category_costs);
		
-- 4. 
-- 4.1
select distinct
	f.title
from 
	film f
where 
	not exists (select 1 from inventory i where i.film_id = f.film_id)
order by 
	title;

-- 4.2
select distinct
	f.title
from 
	film f
left join 
	inventory i 
	on i.film_id = f.film_id
where
	i.inventory_id is null
order by 
	title;

-- 4.3
with films_except
as
(
	select film_id from film
	except 
	select film_id from	inventory
)
select distinct 
	f.title
from 
	film f
where 
	exists (select 1 from films_except fi where fi.film_id = f.film_id)
order by 
	title;

-- 5. 
with actor_appearance
as
(
	select 
		a.first_name,
		a.last_name,
		count(*) as appearance,
		dense_rank() over (order by count(*) desc) as appearance_rank
	from 
		actor a 
	inner join
		film_actor fa 
		on fa.actor_id = a.actor_id
	inner join
		film f 
		on f.film_id = fa.film_id
	inner join
		film_category fc 
		on fc.film_id = f.film_id
	inner join
		category cat
		on cat.category_id = fc.category_id
	where
		cat.name = 'Children'
	group by 
		a.first_name,
		a.last_name
)
select
	first_name,
	last_name,
	appearance
from
	actor_appearance
where 
	appearance_rank <= 3;

-- 6.  
select
	c.city,
	sum(case when cust.active = 1 then 1 else 0 end) as isActive,
	sum(case when cust.active = 0 then 1 else 0 end) as isInactive
from 
	customer cust
inner join
	address a
	on a.address_id = cust.address_id
inner join
	city c		
	on c.city_id = a.city_id
group by
	city
order by 
	isInactive desc, city;

-- 7. 
with category_cities
as
(
	select 
		cat.name as category_name,
	--	sum(case when city.city like 'a%' then age(r.return_date,r.rental_date) else null end) as rental_inerval_1,
	--	sum(case when city.city like '%-%' then age(r.return_date,r.rental_date) else null end) as rental_inerval_2,
		sum(case when city.city like 'a%' then f.rental_duration else 0 end) as rental_duration_1,
		sum(case when city.city like '%-%' then f.rental_duration else 0 end) as rental_duration_2
	from 
		category cat
	inner join
		film_category fc 
		on fc.category_id = cat.category_id
	inner join
		film f 
		on f.film_id = fc.film_id
	inner join
		inventory i
		on i.film_id = f.film_id
	inner join
		rental r
		on r.inventory_id = i.inventory_id
	inner join
		customer c
		on c.customer_id = r.customer_id
	inner join
		address a
		on a.address_id = c.address_id
	inner join
		city
		on city.city_id = a.city_id
	where
		city.city like 'a%'
		or city.city like '%-%'
	group by 
		cat.name
)
select
	category_name,
	rental_duration_1 as rental_duration,
	'for "a" - cities' as description
from
	category_cities
where
	rental_duration_1 = (select max(rental_duration_1) from category_cities)
union all
select
	category_name,
	rental_duration_2 as rental_duration,
	'for "-" - cities' as description
from
	category_cities
where
	rental_duration_2 = (select max(rental_duration_2) from category_cities);
