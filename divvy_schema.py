from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import expr, count
import os

# Create Spark session
spark = SparkSession.builder.appName("Divvy_Trips_2015-Q1").getOrCreate()

# Get the path to the current directory
current_directory = os.path.dirname(os.path.realpath(__file__))

# Path to the CSV file (same directory as the script)
csv_file = os.path.join(current_directory, "Divvy_Trips_2015-Q1.csv")

# 1. Infer Schema (automatically infer schema)
df_infer = spark.read.option("header", True).csv(csv_file)
print("Infer Schema:")
df_infer.printSchema()
df_infer.select(count(expr("*")), count(df_infer.trip_id)).show()

# 2. Programmatic Schema (using StructFields)
schema_programmatic = StructType([
    StructField("trip_id", IntegerType(), False),
    StructField("starttime", TimestampType(), False),
    StructField("stoptime", TimestampType(), False),
    StructField("bikeid", IntegerType(), False),
    StructField("tripduration", IntegerType(), False),
    StructField("from_station_id", IntegerType(), False),
    StructField("from_station_name", StringType(), False),
    StructField("to_station_id", IntegerType(), False),
    StructField("to_station_name", StringType(), False),
    StructField("usertype", StringType(), False),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

df_programmatic = spark.read.option("header", True).schema(schema_programmatic).csv(csv_file)
print("Programmatic Schema:")
df_programmatic.printSchema()
df_programmatic.select(count(expr("*")), count(df_programmatic.trip_id)).show()

# 3. DDL Schema
schema_ddl = "`trip_id` INT NOT NULL, `starttime` TIMESTAMP NOT NULL, `stoptime` TIMESTAMP NOT NULL, `bikeid` INT NOT NULL, `tripduration` INT NOT NULL, `from_station_id` INT NOT NULL, `from_station_name` STRING NOT NULL, `to_station_id` INT NOT NULL, `to_station_name` STRING NOT NULL, `usertype` STRING NOT NULL, `gender` STRING, `birthyear` INT"

df_ddl = spark.read.option("header", True).schema(schema_ddl).csv(csv_file)
print("DDL Schema:")
df_ddl.printSchema()
df_ddl.select(count(expr("*")), count(df_ddl.trip_id)).show()

# Stop Spark session
spark.stop()