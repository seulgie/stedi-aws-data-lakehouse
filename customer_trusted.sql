CREATE EXTERNAL TABLE IF NOT EXISTS customer_trusted (
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
LOCATION 's3://stedi-project/trusted/customers/';

-- Filter records to only include customers who agreed to share data for research
INSERT INTO customer_trusted
SELECT * FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;
