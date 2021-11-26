use pagila;

create table public.users (id int, name varchar(64));

insert into public.users values (1, 'Anne');
insert into public.users values (2, 'Vasily');
insert into public.users values (3, 'Petr');
insert into public.users values (4, 'Katerina');

select * from public.users;

--------------------------------------------------------------------------
select count(*) from public.film;

select * from public.film;
select * from public.actor;
select * from public.film_actor;
select * from public.category;
select * from public.film_category;
select * from public."language";