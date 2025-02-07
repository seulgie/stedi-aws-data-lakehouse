CREATE EXTERNAL TABLE IF NOT EXISTS 'stedi'.'accelerometer_landing' (
    timeStamp bigint,
    user string,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = "1"
)
LOCATION 's3://stedi-project/accelerometer/';
