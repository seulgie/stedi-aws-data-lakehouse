CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_trusted (
    sensorReadingTime STRING,
    serialNumber STRING,
    distanceFromObject DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = "1"
)
LOCATION 's3://stedi/trusted/step_trainer/';

-- Only include step trainer records from customers who have accelerometer data and agreed to share their data
INSERT INTO step_trainer_trusted
SELECT s.* FROM step_trainer_landing s
JOIN customers_curated c ON s.serialNumber = c.serialnumber;
