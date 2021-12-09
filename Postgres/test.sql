select version();

-- use postgres;

create table test (id int, name varchar(12));

insert into test values (1, 'a');
insert into test values (2, 'b');
insert into test values (3, 'c');

select * from test;
------------------------------------------------------------

create table test2 (id int, name varchar(12));

select * from test2;

truncate table test2;
