import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{expr, count}
import java.nio.file.Paths

object DivvyTrips {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder.appName("Divvy_Trips_2015-Q1").getOrCreate()

    // Get the current directory and path to the CSV file (same directory as the script)
    val current_directory = Paths.get("").toAbsolutePath.toString
    val csv_file = current_directory + "/Divvy_Trips_2015-Q1.csv"

    // 1. Infer Schema (automatically infer schema)
    val df_infer = spark.read.option("header", "true").csv(csv_file)
    println("Infer Schema:")
    df_infer.printSchema()
    df_infer.select(count(expr("*")), count(df_infer("trip_id"))).show()

    // 2. Programmatic Schema (using StructFields)
    val schema_programmatic = StructType(Array(
      StructField("trip_id", IntegerType, false),
      StructField("starttime", TimestampType, false),
      StructField("stoptime", TimestampType, false),
      StructField("bikeid", IntegerType, false),
      StructField("tripduration", IntegerType, false),
      StructField("from_station_id", IntegerType, false),
      StructField("from_station_name", StringType, false),
      StructField("to_station_id", IntegerType, false),
      StructField("to_station_name", StringType, false),
      StructField("usertype", StringType, false),
      StructField("gender", StringType, true),
      StructField("birthyear", IntegerType, true)
    ))

    val df_programmatic = spark.read.option("header", "true").schema(schema_programmatic).csv(csv_file)
    println("Programmatic Schema:")
    df_programmatic.printSchema()
    df_programmatic.select(count(expr("*")), count(df_programmatic("trip_id"))).show()

    // 3. DDL Schema
    val schema_ddl = "trip_id INT NOT NULL, starttime TIMESTAMP NOT NULL, stoptime TIMESTAMP NOT NULL, bikeid INT NOT NULL, tripduration INT NOT NULL, from_station_id INT NOT NULL, from_station_name STRING NOT NULL, to_station_id INT NOT NULL, to_station_name STRING NOT NULL, usertype STRING NOT NULL, gender STRING, birthyear INT"

    val df_ddl = spark.read.option("header", "true").schema(schema_ddl).csv(csv_file)
    println("DDL Schema:")
    df_ddl.printSchema()
    df_ddl.select(count(expr("*")), count(df_ddl("trip_id"))).show()

    // Stop Spark session
    spark.stop()
  }
}
