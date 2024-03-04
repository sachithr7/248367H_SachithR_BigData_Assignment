# 248367H_BigDataAssignment_2024
This repository includes details relating to the Big Data Assignment 

# Hadoop Vs Spark
Hadoop stores and processes data on external storage. Spark stores and process data on internal memory. Hadoop processes data in batches. Spark processes data in real time.

# Architecture:
Hadoop: Hadoop is primarily based on the Hadoop Distributed File System (HDFS) for storage and MapReduce for processing. In Hadoop, data is stored in HDFS, and MapReduce jobs are used to process this data in parallel across a cluster of commodity hardware.
Spark: Spark is a general-purpose cluster computing framework that provides in-memory processing capabilities. It can run on top of various storage systems, including HDFS, Amazon S3, and others. Spark's core abstraction is the Resilient Distributed Dataset (RDD), which allows data to be processed in-memory across distributed nodes.
# Performance:
Hadoop: Hadoop's performance can be limited by the disk I/O involved in reading and writing data to/from HDFS and the intermediate data shuffling in MapReduce jobs.
Spark: Spark's in-memory processing capability makes it significantly faster than Hadoop MapReduce for many workloads, especially those that benefit from iterative algorithms or multiple processing steps.
# Ease of Use:
Hadoop: Writing MapReduce jobs can be complex and requires a deep understanding of distributed computing concepts.
Spark: Spark provides a more user-friendly API and higher-level abstractions (such as DataFrames and Datasets) compared to Hadoop MapReduce, which makes it easier to write and understand code.
# Supported Workloads:
Hadoop: Hadoop MapReduce is well-suited for batch processing of large datasets.
Spark: Spark supports a wider range of workloads, including batch processing, interactive querying, machine learning, stream processing, and graph processing.
# Use Cases:
Hadoop: Hadoop is commonly used for batch processing tasks such as log processing, ETL (Extract, Transform, Load) operations, and data warehousing.
Spark: Spark is suitable for a variety of use cases, including real-time analytics, iterative machine learning algorithms, interactive data analysis, and stream processing.
# Ecosystem:
Hadoop: The Hadoop ecosystem includes various projects such as HBase for NoSQL databases, Hive for SQL-like querying, Pig for data processing, and others.
Spark: Spark has a growing ecosystem with libraries and extensions for different use cases, including MLlib for machine learning, Spark SQL for SQL querying, GraphX for graph processing, and Spark Streaming for real-time analytics.
