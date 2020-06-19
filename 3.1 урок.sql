/*select z.film_id,z.title, z.language_id, l.name
from film z
inner join language l
on z.language_id = l.language_id;

select  f.title, f.film_id,first_name, last_name
from film f
inner join film_actor ff
on ff.film_id = f.film_id
inner join actor a
on ff.actor_id = a.actor_id
where f.film_id = 508
;


select * 
from film

select * 
from film_actor

select * 
from actor

select * 
from film_list

select fid, actors, title
from film_list
where title = 'Lambs Cincinatti'*/








