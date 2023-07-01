-- Question 1
SELECT
EXTRACT (YEAR FROM start_date) AS year,
EXTRACT (MONTH FROM start_date) AS month,
AVG(duration_sec) / 60 AS avg FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE EXTRACT (YEAR FROM start_date) BETWEEN 2014 AND 2017
GROUP BY 1,2
ORDER BY 1,2 ASC;



-- Question 2

SELECT 
EXTRACT (YEAR FROM d.start_date) AS year,
a.name AS region_name,
COUNT(d.trip_id) AS total_trips,
COUNT(distinct c.num_bikes_available) AS total_bikes
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS a
JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS b on b.region_id = a.region_id
JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_status` AS c on c.station_id = b.station_id
JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS d on b.name = d.start_station_name
WHERE EXTRACT (YEAR FROM d.start_date) BETWEEN 2014 AND 2017 
GROUP BY 1,2
ORDER BY 1 ASC;





-- Question 3

WITH table1 AS (
    SELECT
    (2022 - member_birth_year) AS umur,
    member_gender
    FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` 
    GROUP BY 1,2
)
SELECT 
MIN(umur) AS youngest_age,
MAX(umur) AS oldest_age,
member_gender    
FROM table1 as a
WHERE umur is not null
GROUP BY 3;





-- Question 4
WITH temp1 AS(
    SELECT
    a.name AS region_name,
    a.region_id AS region_id,
    b.station_id AS station_id,
    b.name AS station_name
    FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS a
    JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS b on a.region_id = b.region_id
),
temp2 AS(
    SELECT 
    trip_id,
    start_station_id,
    duration_sec,
    start_date,
    start_station_name,
    member_gender
    FROM`bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  

)
SELECT 
--MAX(temp2.start_date) OVER (PARTITION BY temp1.region_id ORDER BY temp1.region_id) AS date,
EXTRACT(YEAR FROM temp2.start_date) AS start_date,
temp2.trip_id AS trip_id,
temp2.duration_sec AS duration_sec,
temp2.start_station_name,
temp2.member_gender,
temp1.region_name
FROM temp1 JOIN temp2 ON temp1.station_name = temp2.start_station_name
WHERE EXTRACT(YEAR FROM temp2.start_date) BETWEEN 2014 AND 2017 AND temp2.member_gender is not null
ORDER BY 1,3 ASC;

 



-- Question 5
WITH temp1 AS(
    SELECT
    a.name AS region_name,
    b.station_id AS station_id,
    b.name AS station_name
    FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS a
    JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS b on a.region_id = b.region_id
),
temp2 AS(
    SELECT 
    start_date,
    EXTRACT (YEAR from start_date) AS year,
    EXTRACT (MONTH from start_date) AS month,
    trip_id,
    start_station_name
    FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`

)

SELECT 
c.start_date,
c.region_name,
c.total_trips, 
SUM(c.total_trips) OVER (PARTITION BY c.region_name ORDER BY c.start_date ASC) AS cum_total_trips
FROM
(SELECT 
DISTINCT EXTRACT (DATE FROM temp2.start_date) AS start_date,
--temp2.year AS year,
--temp2.month AS month,
temp1.region_name AS region_name,
COUNT(trip_id) OVER (PARTITION BY region_name ORDER BY EXTRACT (DATE FROM temp2.start_date) ASC) AS total_trips
FROM temp1 JOIN temp2 ON temp1.station_name = temp2.start_station_name
WHERE EXTRACT (YEAR FROM temp2.start_date) = 2017 AND EXTRACT (MONTH FROM temp2.start_date) BETWEEN 11 AND 12) AS c;



-- Question 7
WITH cohort_items AS (
  SELECT `bigquery-public-data.hacker_news.full`.`by` as user,
  MIN(date(date_trunc(timestamp,MONTH))) as cohort_month,
  FROM `bigquery-public-data.hacker_news.full`
  WHERE type = "story" and EXTRACT(YEAR FROM timestamp) = 2014
  GROUP BY 1
),
user_activities AS (
  SELECT
  a.by as user,
  DATE_DIFF(
    date(date_trunc(a.timestamp,MONTH)),
    b.cohort_month,
    MONTH
  ) AS month_number
  FROM `bigquery-public-data.hacker_news.full` a
  LEFT JOIN cohort_items b ON a.`by` = b.user
  WHERE EXTRACT(YEAR FROM a.timestamp) = 2014 and a.type = "story" and a.by IS NOT NULL 
  GROUP BY 1,2
  
),
cohort_size AS(
  SELECT cohort_month,
  COUNT(*) AS num_users,
  FROM cohort_items
  GROUP BY 1
  ORDER BY 1
),

retention_table AS(
  SELECT 
  d.cohort_month,
  c.month_number,
  COUNT(*) as num_users,
  FROM user_activities c
  LEFT JOIN cohort_items d ON c.user = d.user
  GROUP BY 1,2
  ORDER BY 1,2 ASC
)
SELECT 
e.cohort_month,
f.num_users as cohort_size,
e.month_number,
e.num_users as total_users,
e.num_users / f.num_users
FROM retention_table e
LEFT JOIN cohort_size f ON e.cohort_month = f.cohort_month
ORDER BY 1,3

