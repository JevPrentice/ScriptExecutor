--create table test_table as select now();
insert into test_table (now) values (now());
