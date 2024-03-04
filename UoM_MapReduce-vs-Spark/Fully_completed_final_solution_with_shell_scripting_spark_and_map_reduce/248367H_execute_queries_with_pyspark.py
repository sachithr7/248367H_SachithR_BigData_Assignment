import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import array, lit
import csv
import time

def loadData(data_source, output_uri, output_query):
    with SparkSession.builder.appName("Airline delay Data Analysis").getOrCreate() as spark:
        if data_source is not None:
            df = spark.read.option("header", "true").csv(data_source)

        df.createOrReplaceTempView("delay_flights")
        if output_query == "carrer_delay_iterative":
            executeCarrierDelayQueryIterateFiveTimes(spark, output_uri)
        elif output_query == "carrer_delay":
            executeCarrierDelayQuery(spark, output_uri)
        elif output_query == "nas_delay":
            executeNASDelayQuery(spark, output_uri)
        elif output_query == "weather_delay":
            executeWeatherDelayQuery(spark, output_uri)
        elif output_query == "aircraft_delay":
            executeLateAircraftDelayQuery(spark, output_uri)
        elif output_query == "security_delay":
            executeSecurityDelayQuery(spark, output_uri)

def executeCarrierDelayQueryIterateFiveTimes(spark, output_uri):
    execution_times = []
    for i in range(5):
        start_time = time.time()
        result = spark.sql("SELECT Year as YEAR, SUM((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
    # time in seconds
    rows = [Row(value=value) for value in execution_times]
    df = spark.createDataFrame(rows)
    df.write.option("header", "true").mode("overwrite").csv(output_uri)

def executeCarrierDelayQuery(spark, output_uri):
    result = spark.sql("SELECT Year as YEAR, avg((CarrierDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
    result.write.option("header", "true").mode("overwrite").csv(output_uri)

def executeNASDelayQuery(spark, output_uri):
    result = spark.sql("SELECT Year as YEAR, avg((NASDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
    result.write.option("header", "true").mode("overwrite").csv(output_uri)

def executeWeatherDelayQuery(spark, output_uri):
    result = spark.sql("SELECT Year as YEAR, avg((WeatherDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
    result.write.option("header", "true").mode("overwrite").csv(output_uri)

def executeLateAircraftDelayQuery(spark, output_uri):
    result = spark.sql("SELECT Year as YEAR, avg((LateAircraftDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
    result.write.option("header", "true").mode("overwrite").csv(output_uri)

def executeSecurityDelayQuery(spark, output_uri):
    result = spark.sql("SELECT Year as YEAR, avg((SecurityDelay/ArrDelay)*100) as CarrerDalyPresentage FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year")
    result.write.option("header", "true").mode("overwrite").csv(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    parser.add_argument(
        '--output_query', help="This decides which query to be executed")
    args = parser.parse_args();

    loadData(args.data_source, args.output_uri, args.output_query)