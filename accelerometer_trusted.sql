CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_trusted (
    timeStamp STRING,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = "1"
)
LOCATION 's3://stedi/trusted/accelerometer/';

-- Filter records to only include accelerometer data from customers who agreed to share data for research
INSERT INTO accelerometer_trusted
SELECT * FROM accelerometer_landing
WHERE user IN (SELECT email FROM customer_trusted);
