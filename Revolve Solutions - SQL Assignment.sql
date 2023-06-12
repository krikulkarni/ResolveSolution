--1) Which manufacturer planes had most no of flights? And how many flights?

SELECT DISTINCT p.manufacturer, COUNT(*) OVER(PARTITION BY p.manufacturer) AS flight_count
FROM planes p
INNER JOIN flights f ON p.tailnum::varchar=f.tailnum::varchar
ORDER BY flight_count DESC
LIMIT 1;

--2) Which manufacturer planes had most no of flying hours? And how many hours?

SELECT manufacturer,INTERVAL '1 hour' * total_hour + INTERVAL '1 minute' * total_minute AS total_flying_hours
FROM(
SELECT p.manufacturer, (SUM(CAST(COALESCE(NULLIF(f.hour,'NA'),'0')AS INTEGER)) +   
						  SUM(CAST(COALESCE(NULLIF(f.minute,'NA'),'0')AS INTEGER))/60) AS total_hour,
						  SUM(CAST(COALESCE(NULLIF(f.minute,'NA'),'0')AS INTEGER))%60 AS total_minute
FROM flights f
JOIN planes p ON p.tailnum::varchar=f.tailnum::varchar
GROUP BY p.manufacturer)foo
ORDER BY total_flying_hours DESC
LIMIT 1;

--3) Which plane flew the most number of hours? And how many hours?

SELECT tailnum,INTERVAL '1 hour' * total_hour + INTERVAL '1 minute' * total_minute AS total_flying_hours
FROM(
SELECT p.tailnum, (SUM(CAST(COALESCE(NULLIF(f.hour,'NA'),'0')AS INTEGER)) +   
						  SUM(CAST(COALESCE(NULLIF(f.minute,'NA'),'0')AS INTEGER))/60) AS total_hour,
						  SUM(CAST(COALESCE(NULLIF(f.minute,'NA'),'0')AS INTEGER))%60 AS total_minute
FROM planes p
JOIN flights f ON p.tailnum::varchar=f.tailnum::varchar
GROUP BY p.tailnum) foo
ORDER BY total_flying_hours DESC
LIMIT 1;

--4) Which destination had most delay in flights?

SELECT * FROM airports 
WHERE IATA_CODE LIKE (SELECT destination FROM (
		SELECT SUM(CAST(COALESCE(NULLIF(arr_delay,'NA'),'0')AS INTEGER)) AS delay_flights,dest AS destination
		FROM flights 
		GROUP BY destination ORDER BY delay_flights DESC LIMIT 1) foo)

--5) Which manufactures planes had covered most distance? And how much distance?

SELECT manufacturer,p.tailnum, SUM(distance) AS total_distance
FROM flights f
JOIN planes p ON p.tailnum::varchar=f.tailnum::varchar
GROUP BY manufacturer,p.tailnum
ORDER BY total_distance DESC
LIMIT 1;

SELECT  manufacturer,tailnum,total_distance FROM (
		SELECT *, DENSE_RANK() OVER(PARTITION BY manufacturer ORDER BY total_distance DESC) AS ranking 
		FROM (
				SELECT DISTINCT manufacturer,p.tailnum, SUM(distance) OVER(PARTITION BY manufacturer, p.tailnum) AS total_distance
				FROM flights f
				JOIN planes p ON p.tailnum::varchar=f.tailnum::varchar
				)foo
		)foo2
		WHERE ranking =1

--6) Which airport had most flights on weekends?

SELECT * FROM airports 
WHERE IATA_CODE::VARCHAR IN (SELECT dest FROM (
		SELECT *,DENSE_RANK() OVER (ORDER BY num_flights DESC) AS ranking
		FROM (
				SELECT  COUNT(*) AS num_flights,dest
				FROM flights
				WHERE day IN (6, 0) -- 0 represents Sunday, 6 represents Saturday
				GROUP BY dest
			)foo 
		)foo2 WHERE ranking=1)

