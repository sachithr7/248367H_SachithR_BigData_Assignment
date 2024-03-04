#!/bin/bash

LOAD_HIVE_DATA="CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (
    Id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime FLOAT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime FLOAT,
    CRSElapsedTime INT,
    AirTime FLOAT,
    ArrDelay FLOAT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn FLOAT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay FLOAT,
    WeatherDelay FLOAT,
    NASDelay FLOAT,
    SecurityDelay FLOAT,
    LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'flightdata.csv';"
S3_SPARK_INPUT_PATH="s3://bigdataassignment2024/data/flightdata.csv"
QUERY="SELECT Year as YEAR, SUM((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
NUM_ITERATIONS=5
CSV_FILE="execution_times_common_query_iterator.csv"


calculate_spark_and_hive_query_execution_time() {
    echo "Iteration,Spark Execution Time,Hive Execution Time" >> $CSV_FILE
    for (( i=1; i<=$NUM_ITERATIONS; i++ )); do
        echo "Running Spark query iteration $i..."
        
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY")
EOF
        spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        spark_execution_time=$(( (spark_end_time - spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query iteration $i..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        hive_start_time=$(date +%s%N)
        hive -e "$QUERY" > /dev/null
        hive_end_time=$(date +%s%N)
        hive_execution_time=$(( (hive_end_time - hive_start_time) / 1000000000 )) # in seconds
        echo "$i,$spark_execution_time,$hive_execution_time" >> $CSV_FILE
    done
}



calculate_spark_and_hive_query_execution_time
