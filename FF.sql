/*
4*. Между какими городами(!) пассажиры делали пересадки
*/
select
	distinct on                                 /* Ниже представлен запрос по созданию представления transfer*/
	(final_departure_city,arrival_city)         /* Читать его лучше в порядке пронумерованном ниже, так проще понять*/
	final_departure_city,arrival_city           /*Выбрать все города, между которыми значение трансфер не null*/
from
	transfer
where
	transfer is not null;                                           
 
                                                                 /* 3 */ 
create or replace view transfer as           /*Данной представление используется только для выявление перелетов, в которых*/
select	                                     /* присутсвовал трансфер, перелеты без пересадон определяются Where transfer is null*/
	ticket_no,                               /* А так же, чтоб упростить дальнейшие запросы*/         
	flight_id,
	departure_city,                                         
	case
		when final_departure_city = departure_city then null
		else final_departure_city
	end,
	arrival_city,
		case
		when final_departure_city = arrival_city then null         /* Необходимо для отсечение полетов,  */ 
		else transfer                                              /* с билетами туда-обратно, что сложно интерпретировать*/
	end,	                                                       /* как трансфер ( Прим: Мск- Питер, Питер - Мск) */
	scheduled_departure,
	scheduled_arrival
from                                                               
	(                                                               /* 2 */
	select
		num_flight,
		ticket_no,
		flight_id,
		departure_city,
		arrival_city,
		scheduled_departure,
		scheduled_arrival, 
		lag(arrival_city)                                          /*Transfer для вывода города,*/
		over(partition by ticket_no ) as transfer,                 /* в котором была совершена пересадка */
		lag(departure_city)                                        /*final_departure_city, для того, чтоб определить*/
		over(partition by ticket_no ) as final_departure_city      /*первоначальный пункт назначения */
	from
		(                                                           
		select                                                    /* 1 */
			tf.flight_id,
			tf.ticket_no,
			departure_airport,
			arrival_airport,                                  /*Формирование таблицы с нужными данными и дальнейшей сортировкой*/
			scheduled_departure,
			scheduled_arrival,
			count(tf.flight_id)                                  /*Переменная для подсчета кол-ва пересадок, в том числе*/
			over (partition by tf.ticket_no) as num_flight,     /* при полетах туда- обратно( Прим: Мск- Питер, Питер - Мск)*/
			a1.city as departure_city,                           /*Заведение значение город для отправки*/                     
			a2.city as arrival_city                              /* и прибытия */
		from
			ticket_flights tf
		inner join flights f on
			tf.flight_id = f.flight_id                          /* Джоины для формирование таблицы*/
		inner join airports a1 on
			departure_airport = a1.airport_code
		inner join airports a2 on
			arrival_airport = a2.airport_code
		order by
			ticket_no,                                      /*Сортировка сначала по номеру билета, потом по времени.*/
			scheduled_departure asc                         /*Для определения последовательности перелетов */
			) as transfer_name
		) as route

		
		/*
4.1* При полетах между какими городами делают пересадки чаще?
*/
select * from(
select
	final_departure_city,
	arrival_city,
	num_transfer,
	avg(num_transfer) over() as avg_transf
from
	(
	select
		distinct on
		(final_departure_city,                             /*Из представления берутся данные о городах, между которыми есть пересадка*/
		arrival_city) final_departure_city,                /*Удаляются дублирующиеся строки*/
		arrival_city,                                      /* Вычистялется среднее значение пересадок всеми между городами*/
		count(transfer) 
		over (partition by (final_departure_city,arrival_city)) as num_transfer
	from
		transfer
	where
		transfer is not null) as num_transf_a
		) as num_transf
where
	num_transfer > avg_transf                             /*Сравнение кол-ва пересадок на маршруте со средним*/
order by
	num_transfer desc
	limit 20
	
	/*
4.2* Какие города используют для пересадок чаще?
*/
select * from (
select distinct on (transfer) transfer, count(transfer) over (partition by transfer)as Count_T
from transfer) as foo
order by Count_T desc
limit 10


select *
from
	(
	select
	distinct on (final_departure_city,arrival_city)
		final_departure_city,                            /*вычисление времени ожидания путем вычитания из времени прилета в трансферный город*/
		arrival_city,                                    /*Времени отлета из трансферного города*/
		transfer,
		scheduled_arrival_pre,            
		(timestamptz (scheduled_departure) - timestamptz (scheduled_arrival_pre)) as time_transfer  
	from
		(
		select
			transfer,
			ticket_no,
			departure_city,                                      /*Таблица с первичными данными*/
			final_departure_city,
			arrival_city,
			scheduled_departure,
			scheduled_arrival,
			lag(scheduled_arrival) over(partition by ticket_no ) as scheduled_arrival_pre         /*Время прилета в трансферный город*/
		from
			transfer) as scheduled_arrival_pre) as goo
where
	transfer is not null
	order by time_transfer desc
	offset 100                               /* Если не вводить значение offset, то выдаются значения 16,17,18 дней, что скорее*/
	limit 20                                 /* можно характеризовать как один из пунктов путешествия, а не трансфер, для определения*/
                                             /* макс значения, нужна регламентация о макс времени трансфера, чтоб разграничивать эти понятия*/
