#!/usr/bin/env python3

# Lancer avec la commande:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 exo8_kafka.py

import sys

import pyspark
import pyspark.sql



def print_stream(df, mode="append", **options):
    writer = df.writeStream
    writer.outputMode(mode)
    writer.format("console")
    writer.options(options)
    return writer.start()


def kafka_output(df, mode="append", **options):
    writer = df.writeStream
    writer.outputMode(mode)
    writer.format("console")
    writer.options(options)
    return writer.start()

def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    kafkareader = spark.readStream.format("kafka")
    kafkareader.option("kafka.bootstrap.servers", "localhost:9092")
    kafkareader.option("subscribe", "chat_spark_moderation")
    df = kafkareader.load()

    df = df.selectExpr("CAST(key AS STRING) AS url", "CAST(value AS STRING) AS ip", "timestamp")
    df = df.withWatermark("timestamp", "1 second")
    print_stream(df, truncate=False)

    win = pyspark.sql.functions.window("timestamp", "5 second", "1 second")
    df = df.groupBy("url", "ip", win).agg({})
    df = df.groupBy("url", "window").count()
    df = df[df["count"] >= 5]
    #df = df.sort("count", ascending=False)

    stream = print_stream(df, truncate=False, numrows=100)
    stream.awaitTermination()


if __name__ == "__main__":
    main()
