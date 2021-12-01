use pagila;

select count(*) from payment p;		-- 16 049

select * from payment p;

select version();

select * from test;

insert into test values (1, 'a');
insert into test values (2, 'b');
insert into test values (3, 'c');

create table test2 (id int, name varchar(12));

select * from test2;

truncate table test2;
