
CREATE TABLE bookings.results ( id int, response text);

--1
insert into results 
select 1 id,     max(c_pid) max_id 
from (  SELECT book_ref, count(passenger_id) c_pid 
		FROM bookings.tickets 
		group by book_ref) t;

	
--2		
insert into results 
select 2 id, count(book_ref) c_book 
from (	select book_ref, c_pid, avg(c_pid) over ()  r 
		from (	SELECT book_ref, count(passenger_id) c_pid 	
				FROM bookings.tickets 
				group by book_ref) t ) c 
where c_pid > r;

--3		--результат NULL запись в таблицу не далаю.
select book_ref
from (	select bt.book_ref, passenger_name, count(passenger_name) cpn
		FROM bookings.tickets bt join ( select book_ref
					 from (	SELECT book_ref, count(passenger_id) c_pid
                                              				FROM bookings.tickets
                                              				group by book_ref) g
					where c_pid = (select max(c_pid) from (SELECT book_ref, count(passenger_id) c_pid
                                              			FROM bookings.tickets
                                              			group by book_ref) k)) h on bt.book_ref = h.book_ref
		group by bt.book_ref, passenger_name) k
where cpn > 1;


--4
insert into results
select 4 id, concat(tik.book_ref, '|', passenger_id,'|', passenger_name,'|', contact_data) result
		FROM bookings.tickets tik left join (
										select  book_ref, count(passenger_id) cpid
										from bookings.tickets
										group by book_ref
										) count_pid on tik.book_ref = count_pid.book_ref
		where count_pid.cpid = 3;


--5
insert into results
select 5 id, max(c)
from (	SELECT  bt.book_ref, count(bf.flight_id) c 
		FROM bookings.tickets bt join bookings.ticket_flights btf on bt.ticket_no = btf.ticket_no
								 join bookings.flights bf on btf.flight_id = bf.flight_id
		where bf.status in ('Departed', 'Arrived', 'On Time')
		group by bt.book_ref) c_bf;

--6
insert into results
select 6 id, max(cp)
from (	select book_ref, max(c) cp
		from (	SELECT  bt.book_ref, passenger_id, count(bf.flight_id) c 
				FROM bookings.tickets bt join bookings.ticket_flights btf on bt.ticket_no = btf.ticket_no
										 join bookings.flights bf on btf.flight_id = bf.flight_id
				where bf.status in ('Departed', 'Arrived', 'On Time')
				group by bt.book_ref, passenger_id) c_bf
		group by book_ref) mbr;


--7
insert into results
select 7 id, max(c)
from (	SELECT  passenger_id, count(bf.flight_id) c 
		FROM bookings.tickets bt join bookings.ticket_flights btf on bt.ticket_no = btf.ticket_no
								 join bookings.flights bf on btf.flight_id = bf.flight_id
		where bf.status in ('Departed', 'Arrived', 'On Time')
		group by passenger_id) c_bf;

--8
insert into results
SELECT 8 id, concat_ws('|', passenger_id, passenger_name, contact_data, sam) p
FROM (
         SELECT bt.passenger_id
              , bt.passenger_name
              , bt.contact_data
              , SUM(btf.amount) sam
              , RANK() OVER (ORDER BY SUM(btf.amount)) arank
         FROM bookings.tickets bt
             JOIN bookings.ticket_flights btf ON bt.ticket_no = btf.ticket_no
             JOIN bookings.flights bf ON btf.flight_id = bf.flight_id
         WHERE bf.status != 'Cancelled'
         GROUP BY bt.passenger_id, bt.passenger_name, bt.contact_data
         ORDER BY SUM(btf.amount)
     ) g
WHERE arank = 1;


--9
insert into results
SELECT 9 id, concat_ws('|', passenger_id, passenger_name, contact_data, smin) p
FROM (
         SELECT bt.passenger_id
              , bt.passenger_name
              , bt.contact_data
              , SUM(bf.actual_duration) smin
              , RANK() OVER (ORDER BY SUM(bf.actual_duration) DESC) arank
         FROM bookings.tickets bt
             JOIN bookings.ticket_flights btf ON bt.ticket_no = btf.ticket_no
             JOIN bookings.flights_v bf ON btf.flight_id = bf.flight_id
         WHERE bf.status = 'Arrived'
         GROUP BY bt.passenger_id, bt.passenger_name, bt.contact_data
         ORDER BY smin DESC
     ) g
WHERE arank = 1;

--10
insert into results 
select 10 id, city
from (		
		select city, count(airport_code)  mair
		from(		SELECT airport_code, x.ru city
					FROM bookings.airports_data
					,json_to_record(city::json) x (ru text)) ac
		group by city
		having count(airport_code) > 1 ) totair;

--11
insert into results
SELECT 11 id, departure_city
FROM (
         SELECT COUNT(DISTINCT arrival_city) c
              , departure_city
              , RANK() OVER (ORDER BY COUNT(DISTINCT arrival_city)) arank
         FROM bookings.routes
         GROUP BY departure_city
     ) g
WHERE arank = 1;
	
--12
insert into results
SELECT 12  id, concat(c1, '|', c2) p
FROM (   
         SELECT dep.city c1, arr.city c2
         FROM bookings.airports dep
            , bookings.airports arr
         WHERE dep.city != Arr.city 
         EXCEPT
         SELECT dep.city c1, arr.city c2
         FROM bookings.flights f
            , bookings.airports dep
            , bookings.airports arr
         WHERE f.departure_airport = dep.airport_code
           AND f.arrival_airport = arr.airport_code
     ) t
WHERE c1 < c2;

--13
insert into results
SELECT DISTINCT 13 id, arrival_city
FROM bookings.routes br
WHERE arrival_city NOT IN (
        SELECT arrival_city
        FROM bookings.routes br2
        WHERE br2.departure_city = 'Москва')
        AND arrival_city != 'Москва';


--14
insert into results
with airc as (	SELECT modair, count(flight_id) cfli
				FROM bookings.flights bf left join (SELECT aircraft_code, x.ru modair
												FROM bookings.aircrafts_data
												,json_to_record(model::json) x (ru text)) bac on bf.aircraft_code = bac.aircraft_code	
				where actual_departure is not null
				group by modair )
select 14 id, modair
from airc
where cfli = (select max(cfli) from airc);


--15
insert into results
with tc as (SELECT modair, count(passenger_id) cpid
			FROM bookings.tickets bt 	left join bookings.ticket_flights btf on bt.ticket_no = btf.ticket_no 
										left join bookings.flights bf on btf.flight_id = bf.flight_id 
										left join (SELECT aircraft_code, x.ru modair
													FROM bookings.aircrafts_data
													,json_to_record(model::json) x (ru text)) bac on bf.aircraft_code = bac.aircraft_code
			where actual_departure is not null
			group by modair)
select 15 id, modair
from tc
where cpid = (select max(cpid) from tc);


--16
insert into results
SELECT 16 id, abs(extract(epoch from sum(scheduled_duration) - sum(actual_duration)) / 60)::int  d
FROM bookings.flights_v
WHERE status = 'Arrived';


--17   --результат NULL запись в таблицу не далаю.
select distinct city_ar 
FROM bookings.tickets bt 	left join bookings.ticket_flights btf on bt.ticket_no = btf.ticket_no 
							left join bookings.flights bf on btf.flight_id = bf.flight_id 
							left join 	(SELECT airport_code, x.ru city_dep
										 FROM bookings.airports_data
										 ,json_to_record(city::json) x (ru text)) badep on bf.departure_airport = badep.airport_code
							left join 	(SELECT airport_code, x.ru city_ar
										 FROM bookings.airports_data
										,json_to_record(city::json) x (ru text)) badar on bf.arrival_airport = badar.airport_code
where 	cast(actual_departure as date) = '2016-09-13'
	and	city_dep = 'Санкт-Петербург';
	
--18
insert into results
with tfli as (	select btf.flight_id , sum(amount) samount
				FROM bookings.ticket_flights btf left join bookings.flights bf on btf.flight_id = bf.flight_id 
				where actual_departure is not null
				group by btf.flight_id)
select 18 id, flight_id
from tfli
where samount = (select max(samount) from tfli);

--19
insert into results
with gdte as (	select dte, count(flight_no) cfl
				from (	select flight_no, cast(actual_departure as date) as dte
						FROM bookings.flights  
						where actual_departure is not null) fldt
				group by dte)
select 19 id, dte
from gdte
where cfl = (select min(cfl) from gdte);


--20	--результат NULL запись в таблицу не далаю.
select 20 id, avg(cfl)
from (	select dte, count(flight_no) cfl
		from (  select cast(actual_departure as date) dte, flight_no
				FROM bookings.flights bf left join 	(	SELECT airport_code, x.ru city_dep
														FROM bookings.airports_data
														,json_to_record(city::json) x (ru text)) badep on bf.departure_airport = badep.airport_code
				where 	actual_departure is not null
						and cast(actual_departure as date) between  '2016-09-01' and '2016-09-30'
						and  city_dep = 'Москва') dtfl
group by dte) dtcfl;


--21
insert into results
select 21 id, city_dep
from (	select city_dep, avg(tm) mimut
		from (  select city_dep, (actual_arrival - actual_departure) tm
				FROM bookings.flights bf left join 	(	SELECT airport_code, x.ru city_dep
														FROM bookings.airports_data
														,json_to_record(city::json) x (ru text)) badep on bf.departure_airport = badep.airport_code
				where 	actual_departure is not null) dtfl
		group by city_dep
		having avg(tm) > '03:00:00'
		order by mimut desc
		limit 5) g ;