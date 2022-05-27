CREATE TABLE results (id INT,
					 response TEXT);



INSERT INTO results (id, response)
VALUES (1, 
(SELECT count(passenger_id)
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref
ORDER BY count(passenger_id) DESC
LIMIT 1));



INSERT INTO results (id, response)
VALUES (2, 
(SELECT COUNT(book_ref)
FROM 
(SELECT b.book_ref, count(passenger_id)
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref
HAVING count(passenger_id) > 
(SELECT AVG(count_pas)
FROM
(SELECT count(passenger_id) as count_pas
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref) t)
order by count(passenger_id)) t2));



INSERT INTO results (id, response)
VALUES (3, 
(SELECT COUNT(*)
FROM
(WITH src AS
(SELECT t1.book_ref, STRING_AGG(passenger_id, ', ') AS reserve_signature FROM 
(SELECT b.book_ref, passenger_id
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
WHERE b.book_ref IN 
(SELECT b.book_ref
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref
HAVING count(passenger_id) = 
(SELECT count(passenger_id)
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref
ORDER BY count(passenger_id) DESC
LIMIT 1))) t1
GROUP BY 1
ORDER BY reserve_signature)
SELECT book_ref FROM src WHERE reserve_signature IN 
(SELECT reserve_signature
 FROM src
 GROUP BY 1
 HAVING COUNT(*) >= 2)) t5));



INSERT INTO results
SELECT *
FROM
(SELECT 4, concat_ws('|', book_ref::text, 
				  passenger_id::text, 
				  passenger_name::text, 
				  contact_data::text) AS res
FROM
(SELECT b.book_ref, passenger_id, passenger_name, contact_data
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
WHERE b.book_ref IN
(SELECT b.book_ref
FROM bookings b INNER JOIN tickets t ON b.book_ref=t.book_ref
GROUP BY b.book_ref
HAVING COUNT(passenger_id) = 3)
ORDER BY b.book_ref, passenger_id, passenger_name, contact_data) fo) foo;



INSERT INTO results (id, response)
VALUES (5, 
(SELECT count(flight_id)
FROM bookings b INNER JOIN tickets t ON b.book_ref = t.book_ref
INNER JOIN ticket_flights tf ON tf.ticket_no=t.ticket_no
GROUP BY b.book_ref
ORDER BY count(flight_id) DESC
LIMIT 1));



INSERT INTO results (id, response)
VALUES (6, 
(SELECT count(flight_id)
FROM bookings b INNER JOIN tickets t ON b.book_ref = t.book_ref
INNER JOIN ticket_flights tf ON tf.ticket_no=t.ticket_no
GROUP BY b.book_ref, passenger_id
ORDER BY count(flight_id) desc
LIMIT 1));



INSERT INTO results (id, response)
VALUES (7,
(SELECT count(*)
FROM flights f INNER JOIN ticket_flights tf ON tf.flight_id = f.flight_id
INNER JOIN tickets t ON tf.ticket_no=t.ticket_no
GROUP BY passenger_id
ORDER BY count(*) DESC
LIMIT 1));



INSERT INTO results
SELECT *
FROM
(SELECT 8, concat_ws('|', passenger_id::text, 
					passenger_name::text, 
					contact_data::text, 
					total_amount::text) AS res
FROM
(SELECT passenger_id, passenger_name, contact_data, SUM(amount) as total_amount
FROM ticket_flights tf INNER JOIN tickets t ON t.ticket_no = tf.ticket_no
INNER JOIN flights f ON tf.flight_id=f.flight_id
WHERE status != 'Cancelled'
GROUP BY passenger_id, passenger_name, contact_data
ORDER BY total_amount
FETCH NEXT 1 ROWS WITH TIES) t
ORDER BY res) foo;



INSERT INTO results
SELECT *
FROM
(SELECT 9, concat_ws('|', passenger_id::text, 
					passenger_name::text, 
					contact_data::text, 
					total_time::text) AS res
FROM
(SELECT passenger_id, passenger_name, contact_data, SUM(actual_duration) as total_time
FROM ticket_flights tf INNER JOIN tickets t ON t.ticket_no = tf.ticket_no
INNER JOIN flights_v f ON tf.flight_id=f.flight_id
WHERE status = 'Arrived'
GROUP BY passenger_id, passenger_name, contact_data
ORDER BY total_time DESC
FETCH NEXT 1 ROWS WITH TIES) t
ORDER BY res) foo;



INSERT INTO results
SELECT *
FROM
(SELECT 10, city
FROM airports
GROUP BY city
HAVING count(*) > 1
ORDER BY city) foo;



INSERT INTO results
SELECT *
FROM
(SELECT 11, *
FROM
(SELECT departure_city
FROM routes
GROUP BY departure_city
ORDER BY count(distinct arrival_city)
FETCH NEXT 1 ROWS WITH TIES) foo
ORDER BY departure_city) fo;



INSERT INTO results
SELECT *
FROM
(SELECT 12, concat_ws('|', departure_city::text,
					 arrival_city::text) AS res
FROM
(SELECT distinct t1.city as departure_city, t2.city as arrival_city
FROM airports t1, airports t2
WHERE t1.city != t2.city
EXCEPT
SELECT distinct departure_city, arrival_city
FROM flights_v
ORDER BY departure_city, arrival_city) foo
WHERE departure_city < arrival_city) moo;



INSERT INTO results
SELECT *
FROM
(SELECT 13, city
FROM
(SELECT distinct city
FROM airports
WHERE city != 'Москва'
EXCEPT
SELECT distinct arrival_city
FROM
(SELECT flight_id, city as departure_city, arrival_airport
FROM airports a INNER JOIN
(SELECT flight_id, departure_airport, arrival_airport
FROM flights) t ON t.departure_airport = a.airport_code) t1
INNER JOIN
(SELECT flight_id, city as arrival_city, departure_airport
FROM airports a INNER JOIN
(SELECT flight_id, departure_airport, arrival_airport
FROM flights) t ON t.arrival_airport = a.airport_code) t2 ON t1.flight_id = t2.flight_id
WHERE departure_city = 'Москва'
ORDER BY city) foo) moo;



INSERT INTO results (id, response)
VALUES (14, 
(SELECT model
FROM aircrafts
WHERE aircraft_code = 
(SELECT aircraft_code
FROM flights
WHERE status='Arrived'
GROUP BY aircraft_code
ORDER BY count(flight_id) desc
LIMIT 1)));



INSERT INTO results (id, response)
VALUES (15, 
(SELECT model
FROM aircrafts
WHERE aircraft_code = 
(SELECT aircraft_code
FROM flights f INNER JOIN ticket_flights tf ON tf.flight_id = f.flight_id
WHERE status='Arrived'
GROUP BY aircraft_code
ORDER BY count(f.flight_id) DESC
LIMIT 1)));



INSERT INTO results (id, response)
VALUES (16, 
(SELECT ABS(EXTRACT (HOUR FROM (SUM(scheduled_duration) - SUM(actual_duration)))) * 60 + 
ABS(EXTRACT (MINUTE FROM (SUM(scheduled_duration) - SUM(actual_duration))))
FROM flights_v
WHERE status = 'Arrived'));



INSERT INTO results
SELECT *
FROM
(SELECT DISTINCT 17, city
FROM airports
WHERE airport_code in 
(SELECT DISTINCT arrival_airport
FROM flights f INNER JOIN airports a ON f.departure_airport = a.airport_code
WHERE status in ('Arrived', 'Departed')
AND extract(day from (scheduled_departure)) = 13
AND extract(month from (scheduled_departure)) = 09
AND extract(year from (scheduled_departure)) = 2016
AND city = 'Санкт-Петербург')
ORDER BY city) foo;



INSERT INTO results
SELECT *
FROM
(SELECT 18, f.flight_id
FROM flights f INNER JOIN ticket_flights tf ON f.flight_id=tf.flight_id
WHERE status != 'Cancelled'
GROUP BY f.flight_id
ORDER BY SUM(amount) desc, flight_id
FETCH NEXT 1 ROWS WITH TIES) foo;



INSERT INTO results
SELECT *
FROM
(SELECT 19, to_char(actual_departure, 'YYYY-MM-DD') as date
FROM flights
WHERE status != 'Cancelled' and actual_departure is not null
GROUP BY date
ORDER BY count(flight_id)
FETCH NEXT 1 ROWS WITH TIES) foo;



INSERT INTO results (id, response)
VALUES (20,
(SELECT COUNT(flight_id) / 30
FROM flights_v
WHERE status in ('Departed', 'Arrived')
AND extract(month from (scheduled_departure)) = 09
AND extract(year from (scheduled_departure)) = 2016
AND departure_city='Москва'));



INSERT INTO results
SELECT *
FROM
(SELECT 21, *
FROM
(SELECT departure_city
FROM flights_v
GROUP BY departure_city
HAVING AVG(actual_duration) > interval '3 hours'
ORDER BY AVG(actual_duration) desc, departure_city
FETCH NEXT 5 ROWS WITH TIES) foo
ORDER BY departure_city) foo;