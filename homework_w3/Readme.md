#Create external table
CREATE OR REPLACE EXTERNAL TABLE `scientific-base-447120-g7.zoomcamp.yellow_taxi_24_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-de-zoomcamp-buckett/yellow_tripdata_2024*.parquet']
);

#Create table
CREATE OR REPLACE TABLE `scientific-base-447120-g7.zoomcamp.yellow_taxi_24` AS
SELECT * FROM `scientific-base-447120-g7.zoomcamp.yellow_taxi_24_external`;

#Q1: counting raws
SELECT count(*) FROM `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`;
#20332093

#Q2: count the distinct number of PULocationIDs of external table
SELECT COUNT(DISTINCT(PULocationID)) FROM  `scientific-base-447120-g7.zoomcamp.yellow_taxi_24_external`;
SELECT COUNT(DISTINCT(PULocationID)) FROM  `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`;
#external: 0MB
#materialized: 155 MB
#Risposta: 0 MB for the External Table and 155.12 MB for the Materialized Table

#Q3: count the distinct number of PULocationIDs of materialized table
SELECT PULocationID, DOLocationID FROM  `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`;
#risposta: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data

#Q4:How many records have a fare_amount of 0?
SELECT count(*) FROM  `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`
WHERE fare_amount = 0;
#8333

#Q5: Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE TABLE `scientific-base-447120-g7.zoomcamp.yellow_taxi_24_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM  `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`;

#Q6
SELECT DISTINCT VendorID
FROM `scientific-base-447120-g7.zoomcamp.yellow_taxi_24_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
#26.84MB
SELECT DISTINCT VendorID
FROM `scientific-base-447120-g7.zoomcamp.yellow_taxi_24`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
#310.24MB

#Q7:GCP Bucket
#Q8: false
