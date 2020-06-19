/*Создание представления для дальнейшей аналитики*/
create or replace
view avg_feedback as 
select
	c_name,
	avg(service) over (partition by c_name) as a_service,
	avg(punctuality) over (partition by c_name) as a_punctuality,
	avg(food) over (partition by c_name) as a_food,
	avg(c_cost) over (partition by c_name) as a_c_cost,
	avg(comfort) over (partition by c_name) as a_comfort,
	(avg(c_cost) over (partition by c_name) + 				/*Вывод среднего значения по каждому параметру*/
	avg(comfort) over (partition by c_name) + 				/*И дальнейшее выявление среднего по всем параметрам для каждой компании*/
	avg(service) over (partition by c_name) + 
	avg(punctuality) over (partition by c_name) + 
	avg(food) over (partition by c_name))/ 5 as av_rate
from
	flights f
inner join flight_company fc on
	fc.flight_id = f.flight_id 									/*Формирование таблицы для аналитики, при помощи джоинов через внешние ключи*/
inner join feedback fe on
	fe.company_code = fc.company_code
inner join company c on
	c.company_code = fc.company_code;							/*Возможна сортировка по любым парам параметров (см ниже) */
	
	
select
	distinct on
	(c_name) c_name ,
	av_rate
from                            							/*Сортировка через Where для выбора лучшего рейтинга по отзывам*/
	avg_feedback 											/*В данном случае мы выбираем по среднему всех параметров*/	
where
		av_rate > 4.5;

	
select * from(	
select
	distinct on
	(c_name) c_name ,
	(a_c_cost+a_comfort)/2 as CF
from                             							
	avg_feedback) as fff
order by CF desc 							/* В данном случае мы сортируем по 2 параметрам, которые нам наиболее интересны*/
limit 5

select * from (
select
	distinct on	(c_name)c_name,
	avg(c_cost) over ( partition by c_name) as avg_rate_cost ,    /*средняя оценка по параметру цена у авиакомпания*/
	avg(amount) over ( partition by c_name) as avg_amount         /*средняя цена */
from
	( select*
from
	flights f
inner join flight_company fc on
	fc.flight_id = f.flight_id
inner join feedback fe on
	fe.company_code = fc.company_code
inner join company c on
	c.company_code = fc.company_code
inner join ticket_flights tf on
	tf.flight_id = fc.flight_id) as avg_c) as avg_c1
	order by avg_amount desc;
	
	select c_name, avg_amount, av_rate, avg(av_rate) over() from (
select
	distinct on	(c_name)c_name,    
	avg(amount) over ( partition by c_name) as avg_amount,
	(avg(c_cost) over (partition by c_name) + 				/*Вывод среднего значения по каждому параметру*/
	avg(comfort) over (partition by c_name) + 				/*И дальнейшее выявление среднего по всем параметрам для каждой компании*/
	avg(service) over (partition by c_name) + 
	avg(punctuality) over (partition by c_name) + 
	avg(food) over (partition by c_name))/ 5 as av_rate 
from
	( select*
from
	flights f
inner join flight_company fc on
	fc.flight_id = f.flight_id
inner join feedback fe on
	fe.company_code = fc.company_code
inner join company c on
	c.company_code = fc.company_code
inner join ticket_flights tf on
	tf.flight_id = fc.flight_id) as avg_c) as avg_c1
	order by avg_amount desc;
	
select distinct on (CT) CT, animals from (
select animals, count(animals) over (partition by animals) as CT from "Animals") as gg;

/* На основе данного запроса можно узнать сколько людей берут с собой питомцев ,а сколько нет*/

select city as arrival_city, count(animals) as ct
from "Animals" a
inner join tickets t 
on t.ticket_no = a.ticket_no
inner join ticket_flights tf on
	tf.ticket_no = t.ticket_no 								
inner join flights f on
	f.flight_id = tf.flight_id
inner join airports air on
	air.airport_code = f.arrival_airport
group by animals,city
Having animals = 'Yes'
order by count(animals) desc;

/* В данном случае была произведена попытка выяснить в какие города чаще всего летают с питомцами*/
/*Но, после выполнения запроса, стало очевидно, что не учитываются пересадки и маршрут туда-обратно
 * Поэтому стоит рассмотреть финальную точку маршрута и проанализировать
 * в какие города прилетают в итоге с питамцами*/




select final_departure_city,count(animals) from transfer t
inner join "Animals" a 
on a.ticket_no = t.ticket_no
group by animals,final_departure_city
Having animals = 'Yes'
order by count(animals) desc;

/* Как видно из таблицы, около 46 тысяч питомцев, пропадают где то на пересадках, это может быть связано с услугами по транспортировке животных
 * В Мосвку же по прежнему летают с животными чаще всего, т.к в целом в Москву и Питер летают чаще*/


select * from (
select distinct on(model) model,count(flight_id) over (partition by model) as ct,date_of_issue from (
select ad.model, ad.date_of_issue, flight_id from aircrafts a
inner join flights f 
on f.aircraft_code = a.aircraft_code
inner join aircraft_data ad 
on ad.aircraft_code = a.aircraft_code) as foo) as goo 
order by ct desc

/*В данном запросе, цель была в получении информации о наиболее используемых самолетах и их сроке выпуска
 * Как видно в таблице, чаще всего используют наиболее устаревшие самолеты, т.е наблюдается низкая скорость замены авиапарка
 * в среднем по всем компаниям*/

select c_name,ct from (
select distinct on(c_name) c_name, count(model) over (partition by c_name) as ct , date_of_issue
from(
select ad.model, ad.date_of_issue, f.flight_id , c.c_name
from aircrafts a
inner join flights f 
on f.aircraft_code = a.aircraft_code
inner join aircraft_data ad 
on ad.aircraft_code = a.aircraft_code
inner join flight_company fc 
on fc.flight_id = f.flight_id
inner join company c 
on c.company_code = fc.company_code) as foo)as goo
where date_of_issue = '2012-11-29'
order by ct desc

/* В данном запросе мы получили информацию о авиокомпаниях, часто использующих наиболее устаревшие самолеты*/

select c_name,ct from (
select distinct on(c_name) c_name, count(model) over (partition by c_name) as ct , date_of_issue
from(
select ad.model, ad.date_of_issue, f.flight_id , c.c_name
from aircrafts a
inner join flights f 
on f.aircraft_code = a.aircraft_code
inner join aircraft_data ad 
on ad.aircraft_code = a.aircraft_code
inner join flight_company fc 
on fc.flight_id = f.flight_id
inner join company c 
on c.company_code = fc.company_code) as foo)as goo
where date_of_issue = '2019-01-20'
order by ct desc

/* В данном запросе мы получили информацию о авиокомпаниях, часто использующих наиболее современные самолеты*/