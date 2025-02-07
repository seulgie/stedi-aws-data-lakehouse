CREATE EXTERNAL TABLE IF NOT EXISTS 'stedi'.'step_trainer_landing' (
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = "1"
)
LOCATION 's3://stedi-project/step_trainer/';
