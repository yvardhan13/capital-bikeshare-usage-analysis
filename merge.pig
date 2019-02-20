data = LOAD 's3://<location>/Capital Bikeshare/201[0-7]-Q[1-4]-cabi-trip-history.csv' USING PigStorage(',') AS (
duration: long,
start_date: chararray,
start_station: chararray,
end_date: chararray,
end_station: chararray,
bike_no: chararray,
member_type: chararray
);

data1 = FOREACH data GENERATE duration, ToDate(start_date, 'MM/dd/yyyy HH:mm') AS start_date_time, start_station, ToDate(end_date, 'MM/dd/yyyy HH:mm') AS end_date_time,end_station, bike_no, member_type;

data2 = FOREACH data1 GENERATE duration, REPLACE(start_date_time, 'Z','') AS start_date_time1, start_station, REPLACE(end_date_time, 'Z','') AS end_date_time1,end_station, bike_no, member_type;

data3 = FOREACH data2 GENERATE duration, TRIM(REPLACE(start_date_time1, 'T',' ')) AS start_date_time2, start_station, TRIM(REPLACE(end_date_time1, 'T',' ')) AS end_date_time2,end_station, bike_no, member_type;

data4 = FOREACH data3 GENERATE duration, SUBSTRING(start_date_time2, 1,22) AS start_date_time3, start_station, SUBSTRING(end_date_time2, 1,22) AS end_date_time3,end_station, bike_no, member_type;

data5 = FOREACH data4 GENERATE duration, CONCAT('2',start_date_time3) AS start_date_time4, start_station, CONCAT('2',end_date_time3) AS end_date_time4,end_station, bike_no, member_type;

data6 = FOREACH data5 GENERATE duration, REPLACE(start_date_time4,'(','') AS start_date_time5, start_station, REPLACE(end_date_time4,'(','') AS end_date_time5,end_station, bike_no, member_type;

data7 = FOREACH data6 GENERATE duration, TRIM(REPLACE(start_date_time5,')','')) AS start_date_time6, start_station, TRIM(REPLACE(end_date_time5,')','')) AS end_date_time6,end_station, bike_no, member_type;

data8 = FOREACH data7 GENERATE duration, TRIM(SUBSTRING(start_date_time6,0,9)) AS start_date,start_date_time6, start_station, TRIM(SUBSTRING(end_date_time6,0,9)) AS end_date,end_date_time6,end_station, bike_no, member_type;


STORE data8 INTO 's3://<location>/capitalbikesharemerge';