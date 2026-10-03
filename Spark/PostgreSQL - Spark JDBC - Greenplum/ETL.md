~~~
# ────────────────────────────────────────────
# SPARK + POSTGRESQL + GREENPLUM
# ────────────────────────────────────────────

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# ────────────────────────────────────────────
# PYSPARK
# ────────────────────────────────────────────

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


# ────────────────────────────────────────────
# ENV — POSTGRESQL
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env"
)


# ────────────────────────────────────────────
# ENV — GREENPLUM
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\Greenplum_config.env"
)


# ────────────────────────────────────────────
# POSTGRESQL
# ────────────────────────────────────────────

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

pg_url = (
    f"jdbc:postgresql://"
    f"{pg_host}:{pg_port}/{pg_database}"
)

pg_driver = "org.postgresql.Driver"


# ────────────────────────────────────────────
# GREENPLUM
# ────────────────────────────────────────────

gp_host = os.getenv("GP_HOST")
gp_port = os.getenv("GP_PORT")
gp_database = os.getenv("GP_NAME")
gp_user = os.getenv("GP_USER")
gp_password = os.getenv("GP_PASSWORD")

gp_url = (
    f"jdbc:postgresql://"
    f"{gp_host}:{gp_port}/{gp_database}"
)

gp_driver = "org.postgresql.Driver"


# ────────────────────────────────────────────
# SPARK
# ────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("AdvertisingEvents")

    # PostgreSQL / Greenplum JDBC Driver
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    .master("local[4]")

    .getOrCreate()
)


# ────────────────────────────────────────────
# LOG LEVEL
# ────────────────────────────────────────────

spark.sparkContext.setLogLevel("ERROR")


# ────────────────────────────────────────────
# CHECK CONNECTION SETTINGS
# ────────────────────────────────────────────

print("PostgreSQL:", pg_url)
print("Greenplum:", gp_url)
print("Spark:", spark.version)
spark.sparkContext.setLogLevel("ERROR")

#----------------------------------------------START Pipeline---------------


df = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "ad_events")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

print(df.toPandas(), '\n')

df.printSchema()





(
    df.write
    .format("jdbc")
    .option("url", gp_url)
    .option("dbtable", "ad_events")
    .option("user", gp_user)
    .option("password", gp_password)
    .option("driver", gp_driver)
    .mode("overwrite")
    .save()
)



df1 = (
    spark.read
    .format("jdbc")
    .option("url", gp_url)
    .option("dbtable", "core.ad_events")
    .option("user", gp_user)
    .option("password", gp_password)
    .option("driver", gp_driver)
    .load()
)




df1.createOrReplaceTempView("df1")

df1.show()


data_mart= spark.sql("""

SELECT
    campaign_id,
    DATE(event_time) AS event_date,
    EXTRACT(HOUR FROM event_time) AS event_hour,

    COUNT(CASE WHEN event_type = 'impression' THEN 1 END) AS impressions,

    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS clicks,
    COUNT(CASE WHEN event_type = 'conversion' THEN 1 END) AS conversions,

    COALESCE(SUM(cost), 0) AS total_cost

FROM df1  

GROUP BY
    campaign_id,
    DATE(event_time),
    EXTRACT(HOUR FROM event_time);
""")



data_mart.createOrReplaceTempView("data_mart")


data_mart.show()



data_mart.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "core.data_mart") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()

print("Загружено в Greenplum")


data_mart.createOrReplaceTempView("data_mart")

check = spark.sql("""
with tt as (
SELECT
campaign_id,
event_date,
event_hour,
impressions,
clicks,
conversions,
total_cost ,

case when count(*) over(PARTITION BY campaign_id, event_date, event_hour) > 1 then 'campaign_id, event_date, event_hour is dub'

when impressions is null then 'impressions is null'

when clicks is null then 'clicks is null'


when conversions is null then 'conversions is null'

when total_cost is null then 'total_cost is null'

when total_cost < 0 then 'total_cost < 0'

end as error_reason

from data_mart

)

SELECT
error_reason

from tt
where error_reason is not null;

""")


check.show()


df1.createOrReplaceTempView("ad_events")
data_mart.createOrReplaceTempView("data_mart")

check_2 = spark.sql("""

with tt as (

SELECT

campaign_id,

date(event_time) as event_date,

extract (hour from event_time) as event_hour,

count(event_type) as r

from ad_events

GROUP BY 

campaign_id,

date(event_time),

extract (hour from event_time)

ORDER BY campaign_id 

),


tt1 as (

SELECT

campaign_id,

event_date,

event_hour,

COALESCE(sum(impressions + clicks + conversions),0) as r1

from data_mart

GROUP BY 

campaign_id,

event_date,

event_hour

ORDER BY campaign_id 

),

tt2 as (

SELECT

tt.campaign_id,

r - r1 as diff

from tt

join tt1

on tt.campaign_id = tt1.campaign_id

and tt.event_date = tt1.event_date

and tt.event_hour = tt1.event_hour

)

SELECT

campaign_id,

diff

from tt2

where diff <>0;

""")




check_2.show()

~~~
