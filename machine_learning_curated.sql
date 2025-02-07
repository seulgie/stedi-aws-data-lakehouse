CREATE EXTERNAL TABLE IF NOT EXISTS machine_learning_curated (
    sensorReadingTime STRING,
    serialNumber STRING,
    distanceFromObject DOUBLE,
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
LOCATION 's3://stedi/curated/machine_learning/';

-- Aggregate Step Trainer and Accelerometer data for the same timestamp
INSERT INTO machine_learning_curated
SELECT s.sensorReadingTime, s.serialNumber, s.distanceFromObject, a.timeStamp, a.user, a.x, a.y, a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a ON s.sensorReadingTime = a.timeStamp;
