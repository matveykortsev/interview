create table users (
	user_id serial primary key,
	age int
);

create table purchases (
	purchase_id serial primary key,
	user_id int,
	item_id int,
	date date
);

create table items (
	item_id serial primary key,
	price int
);


insert into users (age)
values
	(23),
	(25),
	(67),
	(30),
	(22),
	(45);

insert into items (price)
values
	(100),
	(200),
	(10000),
	(5555),
	(321),
	(500),
	(777),
	(666);
	
insert into purchases (user_id, item_id, date)
values
	(1, 3, '2020-05-26'),
	(3, 7, '2021-10-17'),
	(1, 2, '2020-09-02'),
	(5, 5, '2019-05-27'),
	(2, 4, '2020-04-15'),
	(4, 5, '2018-10-15'),
	(6, 1, '2021-01-01'),
	(2, 6, '2021-07-17'),
    (2, 3, '2020-08-12');
	
