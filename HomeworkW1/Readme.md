##QUESTION 3

SELECT 
	"PULocationID", 
    SUM(total_amount) AS total_amount
FROM 
    green_taxi_data g
WHERE 
    DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    "PULocationID"
HAVING 
    SUM(total_amount) > 13000
ORDER BY 
    total_amount DESC;

SELECT *
	FROM public.zones
	WHERE "LocationID" =74 or "LocationID" =75 or "LocationID" =166;

 ###Question 6
