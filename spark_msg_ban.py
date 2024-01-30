#!/usr/bin/env python3

# Lancer avec la commande:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 exo8_kafka.py

import sys

import pyspark
import pyspark.sql

BROKER = "localhost:9092"
TOPIC_INPUT = "chat_spark_moderation"
TOPIC_OUTPUT = "chat_bans"


def print_stream(df, mode="append"):
    return df.writeStream \
        .outputMode(mode) \
        .format("console") \
        .option("truncate", "false") \
        .start()


def kafka_output(df, mode="append"):
    return df.writeStream \
        .outputMode(mode) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("topic", TOPIC_OUTPUT) \
        .start()


def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC_INPUT) \
        .load()

    win = pyspark.sql.functions.window("timestamp", "5 second", "1 second")
    df = df.selectExpr("CAST(key AS STRING) As pseudo", "CAST(value AS STRING) As message", "timestamp") \
        .withWatermark("timestamp", "1 second") \
        .groupBy("pseudo", "message", win).agg({}) \
        .groupBy("pseudo", "window").count()
    df = df[df["count"] > 7]

    stream = print_stream(df)
    kafka_stream = kafka_output(df)

    stream.awaitTermination()
    kafka_stream.awaitTermination()


if __name__ == "__main__":
    main()
