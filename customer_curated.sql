CREATE EXTERNAL TABLE IF NOT EXISTS customers_curated (
    serialnumber STRING,
    sharewithpublicasofdate STRING,
    birthday STRING,
    registrationdate STRING,
    sharewithresearchasofdate STRING,
    customername STRING,
    email STRING,
    lastupdatedate STRING,
    phone STRING,
    sharewithfriendsasofdate STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "serialization.format" = "1"
)
LOCATION 's3://stedi/curated/customers/';

-- Filter records to only include customers with accelerometer data and who agreed to share data
INSERT INTO customers_curated
SELECT c.* FROM customer_trusted c
JOIN accelerometer_trusted a ON c.email = a.user;
