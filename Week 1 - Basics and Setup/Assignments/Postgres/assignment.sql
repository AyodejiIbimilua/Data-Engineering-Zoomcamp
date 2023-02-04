/*
Link to the assignment can be found here: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md
*/

-- Question 3
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime::DATE = '2019-01-15';


-- Question 4 
SELECT lpep_pickup_datetime::DATE FROM
green_taxi_trips t
WHERE  trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips);

-- Question 5 
SELECT COUNT(*) FROM
green_taxi_trips t
WHERE lpep_pickup_datetime::DATE = '2019-01-01' AND (passenger_count = 2 OR passenger_count = 3);

