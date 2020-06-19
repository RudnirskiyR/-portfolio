/*1. Cоставить список клиентов по имени и фамилии вместе с их адресом электронной почты, чтобы отправить благодарность 5 самым дорогим клиентам, которые арендовали фильмы, с оплатой с 10 по 13 апреля,
формат списка:
clients_list
Имя_клиента Фамилия_клиента email address is: e-mail_клиента*/

SELECT first_name, last_name, email, sum(amount) as a_sum
FROM customer c
inner join payment p
on p.customer_id = c.customer_id
group by first_name, last_name, email,payment_date
having payment_date > '2007-04-10' and payment_date < '2007-04-13'
order by sum(amount) desc
limit (5)

/*2. Какова средняя арендная ставка для каждого жанра? (упорядочить по убыванию, среднее значение округлить до сотых)*/

select name, avg_rate
from (
select distinct on (c.name) c.name, round(avg(rental_rate) over (partition by c.name),2) as avg_rate
from category c
inner join film_category fc
on fc.category_id = c.category_id
inner join film f
on f.film_id = fc.film_id
) as category_avg
order by avg_rate desc

/*3. Сколько арендованных фильмов было возвращено в срок, до срока возврата и после, выведите максимальную разницу со сроком?*/

select count(return_aft_time) as num_return_aft_time, max(rent_dur)
from
(select date(return_date) > (date(rental_date) + rental_duration) as return_aft_time,
date(return_date) < (date(rental_date) + rental_duration) as return_bef_time,
date(return_date) = (date(rental_date) + rental_duration) as return_in_time,
date(return_date) - date(rental_date) as rent_dur
from film f
inner join inventory i
on i.film_id = f.film_id
inner join rental r
on r.inventory_id = i.inventory_id) as ret_time
where return_aft_time = true

