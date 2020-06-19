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
