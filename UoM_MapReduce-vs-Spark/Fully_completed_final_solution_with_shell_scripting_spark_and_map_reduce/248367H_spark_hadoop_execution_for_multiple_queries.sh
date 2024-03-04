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
QUERY_1="SELECT Year as YEAR, SUM((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
QUERY_2="SELECT Year as YEAR, SUM((NASDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
QUERY_3="SELECT Year as YEAR, SUM((WeatherDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
QUERY_4="SELECT Year as YEAR, SUM((LateAircraftDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
QUERY_5="SELECT Year as YEAR, SUM((SecurityDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
CSV_FILE="execution_times_for_each_query.csv"


calculate_spark_and_hive_query_execution_time_for_multiple_queries() {
        echo "type,hiveql,sparksql" >> $CSV_FILE

        echo "Running Spark and hadoop for multiple query no 1 ..."
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        echo "Running spark query no 1 ..."
        q1_spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY_1")
EOF
        q1_spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        q1_spark_execution_time=$(( (q1_spark_end_time - q1_spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query 1 ..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        q1_hive_start_time=$(date +%s%N)
        hive -e "$QUERY_1" > /dev/null
        q1_hive_end_time=$(date +%s%N)
        q1_hive_execution_time=$(( (q1_hive_end_time  - q1_hive_start_time) / 1000000000 )) # in seconds
        echo "Carrier Delay Query,$q1_hive_execution_time,$q1_spark_execution_time" >> $CSV_FILE


        echo "Running Spark and hadoop for multiple query no 2 ..."
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        echo "Running spark query no 2 ..."
        q2_spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY_2")
EOF
        q2_spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        
        q2_spark_execution_time=$(( (q2_spark_end_time - q2_spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query 2 ..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        q2_hive_start_time=$(date +%s%N)
        hive -e "$QUERY_2" > /dev/null
        q2_hive_end_time=$(date +%s%N)
        q2_hive_execution_time=$(( (q2_hive_end_time  - q2_hive_start_time) / 1000000000 )) # in seconds
        echo "NAS Delay Query,$q2_hive_execution_time,$q2_spark_execution_time" >> $CSV_FILE


        echo "Running Spark and hadoop for multiple query no 3 ..."
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        echo "Running spark query 3 ..."
        q3_spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY_3")
EOF
        q3_spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        q3_spark_execution_time=$(( (q3_spark_end_time - q3_spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query 3 ..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        q3_hive_start_time=$(date +%s%N)
        hive -e "$QUERY_3" > /dev/null
        q3_hive_end_time=$(date +%s%N)
        q3_hive_execution_time=$(( (q3_hive_end_time - q3_hive_start_time) / 1000000000 )) # in seconds
        echo "Wheather Delay Query,$q3_hive_execution_time,$q3_spark_execution_time" >> $CSV_FILE


        echo "Running Spark and hadoop for multiple query no 4 ..."
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        echo "Running spark query 4 ..."
        q4_spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY_4")
EOF
        q4_spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        q4_spark_execution_time=$(( (q4_spark_end_time - q4_spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query 4 ..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        q4_hive_start_time=$(date +%s%N)
        hive -e "$QUERY_4" > /dev/null
        q4_hive_end_time=$(date +%s%N)
        q4_hive_execution_time=$(( (q4_hive_end_time - q4_hive_start_time) / 1000000000 )) # in seconds
        echo "Late Aircraft Delay Query,$q4_hive_execution_time,$q4_spark_execution_time" >> $CSV_FILE


        echo "Running Spark and hadoop for multiple query no 5 ..."
        spark-shell <<EOF > /dev/null
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().appName("Spark Query Execution").getOrCreate()
        val df = spark.read.option("header", "true").csv("$S3_SPARK_INPUT_PATH")
        df.createOrReplaceTempView("delay_flights")
EOF
        echo "Running spark query 5 ..."
        q5_spark_start_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        val result = spark.sql("$QUERY_5")
EOF
        q5_spark_end_time=$(date +%s%N)
        spark-shell <<EOF > /dev/null
        spark.stop()
EOF
        
        q5_spark_execution_time=$(( (q5_spark_end_time - q5_spark_start_time) / 1000000000 )) # in seconds

        echo "Running Hive query 5 ..."
        hive -e "$LOAD_HIVE_DATA" > /dev/null
        q5_hive_start_time=$(date +%s%N)
        hive -e "$QUERY_5" > /dev/null
        q5_hive_end_time=$(date +%s%N)
        q5_hive_execution_time=$(( (q5_hive_end_time - q5_hive_start_time) / 1000000000 )) # in seconds
        echo "Security Delay Query,$q5_hive_execution_time,$q5_spark_execution_time" >> $CSV_FILE
}


calculate_spark_and_hive_query_execution_time_for_multiple_queries