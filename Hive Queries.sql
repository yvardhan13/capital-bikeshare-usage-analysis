INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/prediction_test'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT start_station, year(start_date) AS Year, month(start_date) AS Month, 
day(start_date) as Day, date_format(start_date,'u') as Weekday, count(*) as Trips 
FROM hive_table 
GROUP BY day(start_date), start_station, year(start_date), month(start_date), date_format(start_date,'u') LIMIT 5000;


--Incoming outgoing ratio
select T1.start_station, T1.outgoing/T2.incoming AS ratio FROM
(select hive_table.start_station, count(*) as outgoing from hive_table group by hive_table.start_station) as T1
JOIN (select hive_table.end_station, count(*) as incoming from hive_table group by hive_table.end_station) as T2
ON T1.start_station = T2.end_station;


--Average duration 
SELECT member_type,AVG(duration) AS avg_duration FROM hive_table GROUP BY member_type;


--Top 10 routes - 
select hive_table.start_station, hive_table.end_station,count(*) AS routecount from hive_table group by hive_table.start_station, hive_table.end_station order by routecount DESC limit 10;


/* over season*/
SELECT Year, sum(`Winter Trips`) AS `Winter Trips` from (SELECT count(*) AS `Winter Trips`,year(start_date) as Year FROM hive_table GROUP BY (start_date) HAVING month(start_date)=1 or month(start_date) = 2 or month(start_date)=12) as t1 GROUP BY (Year) ORDER BY (Year);

/* Number of Spring trips*/
SELECT count(*) AS `Spring Trips` FROM hive_table where month(start_date)=3 or month(start_date) = 4 or month(start_date)=5;

/* Summer Trips*/
SELECT count(*) AS `Summer Trips` FROM hive_table where month(start_date)=6 or month(start_date) = 7 or month(start_date)=8;

/* Fall */
SELECT count(*) AS `Fall Trips` FROM hive_table WHERE month(start_date)=9 or month(start_date) = 10 or month(start_date)=11;

/*over year*/
SELECT Year, Trips FROM (SELECT year(start_date) AS Year, count(*) AS Trips FROM hive_table GROUP BY year(start_date)) AS sub ORDER BY Year;

/*bike count*/
SELECT Year, Bike_count FROM  (SELECT year(start_date) AS Year, COUNT (DISTINCT bike_no) AS Bike_count  FROM hive_table GROUP BY year(start_date)) AS sub ORDER BY Year;

/*rush hour*/
SELECT member_type, count(*) FROM hive_table group by member_type having (hour(start_date_time) >= 6 and hour(start_date_time) <=9) or (hour(start_date_time) >= 15 and hour(start_date_time)<=17);

--night hours
SELECT count(*) FROM hive_table where hour(start_date_time)>=0 and hour(start_date_time)<6;

--Casual or Registered user trip characteristics

SELECT start_date,hour(start_date_time) as h,member_type,COUNT(*) AS Trips
FROM hive_table WHERE member_type='Casual'
GROUP BY start_date,member_type,hour(start_date_time)
ORDER BY start_date,h;

SELECT start_date,hour(start_date_time) as h,member_type,COUNT(*) AS Trips
FROM hive_table WHERE member_type='Registered'
GROUP BY start_date,member_type,hour(start_date_time)
ORDER BY start_date,h;

SELECT start_date,hour(start_date_time) as h,member_type,avg(duration)/60000 AS `Average Duration (Minutes)`
FROM hive_table WHERE member_type='Registered'
GROUP BY start_date,member_type,hour(start_date_time)
ORDER BY start_date,h;

SELECT start_date,hour(start_date_time) as h,member_type,avg(duration)/60000 AS `Average Duration (Minutes)`
FROM hive_table WHERE member_type='Casual'
GROUP BY start_date,member_type,hour(start_date_time)
ORDER BY start_date,h;

SELECT start_date,hour(start_date_time) as h,avg(duration)/60000 AS `Average Duration (Minutes)`
FROM hive_table GROUP BY start_date,hour(start_date_time)
ORDER BY start_date,h;

SELECT start_date, hour(start_date_time) as h, month(start_date) as m, date_format(start_date,'u') as d from hive_table;




