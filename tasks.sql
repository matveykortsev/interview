-- Какую сумму в среднем в месяц тратят:
-- Пользователи в возрастном диапазоне от 18 до 25 лет включительно

select
u.age as age,
avg(i.price) as avg_price
from purchases as p
left join users as u on p.user_id = u.user_id 
left join items as i on p.item_id = i.item_id
where u.age >= 18 and u.age <= 25
group by age;

-- Пользователи в возрастном диапазоне от 26 до 35 лет включительно

select
u.age as age,
avg(i.price) as avg_price
from purchases as p
left join users as u on p.user_id = u.user_id 
left join items as i on p.item_id = i.item_id
where u.age >= 26 and u.age <= 35
group by age;

-- В каком месяце года выручка от пользователей в возрастном диапазоне 35+ самая большая

select
date_part('month', p."date") as month,
sum(i.price) as total_val
from purchases as p
left join users as u on p.user_id = u.user_id 
left join items as i on p.item_id = i.item_id
where u.age >= 35
group by month
order by total_val desc
limit 1;


-- Какой товар обеспечивает дает наибольший вклад в выручку за последний год

select 
i.item_id as item,
sum(i.price) as total_val,
sum(i.price) * 1.0 / sum(sum(i.price)) over () as percentage
from purchases as p 
join items as i on p.item_id = i.item_id
where p.date >= '2021-01-01'
group by i.item_id 
order by percentage desc
limit 1;

-- Топ-3 товаров по выручке и их доля в общей выручке за любой год

select 
i.item_id as item,
sum(i.price) as total_val,
sum(i.price) * 1.0 / sum(sum(i.price)) over () as percentage
from purchases as p 
join items as i on p.item_id = i.item_id
group by i.item_id 
order by total_val desc
limit 3;


