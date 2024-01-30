#!/usr/bin/env python3

# Lancer avec la commande:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 exo8_kafka.py

import sys

import pyspark
import pyspark.sql

BROKER = "localhost:9092"
TOPIC_INPUT = "chat_spark_moderation"
TOPIC_OUTPUT = "chat_bans"


# Pour le debug local (affichage en terminal)
def print_stream(df, mode="append"):
    return df.writeStream \
        .outputMode(mode) \
        .format("console") \
        .option("truncate", "false") \
        .start()


# Ecriture dans le topic chat_bans
def kafka_output(df, mode="append"):
    return df.writeStream \
        .outputMode(mode) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("topic", TOPIC_OUTPUT) \
        .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint") \
        .start()


def main():
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    """
        Partie responsable pour la connexion de Spark 
        au Topic chat_spark_moderation 
    """
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC_INPUT) \
        .load()

    """
        Définition de la fenêtre temporel 
    """
    win = pyspark.sql.functions.window("timestamp", "5 second", "1 second")
    """
        Formattage du Data-Frame
    """
    # Recuperation du Pseudo , Message , et la date de l'envoi
    df = df.selectExpr("CAST(key AS STRING) As pseudo", "CAST(value AS STRING) As message", "timestamp")
    df = df.withWatermark("timestamp", "1 second")
    # Grouper les messages de chaque Pseudo pour la fenêtre définie
    df = df.groupBy("pseudo", "message", win).agg({})
    # Calculer le nombre de message de chaque pseudo dans la fenêtre temporelle
    df = df.groupBy("pseudo", "window").count()
    # Filtrer, et garder seulement ceux qui envoient plus de 7 messages dans la fenêtre temporelle
    df = df[df["count"] > 7]

    # WriteStream pour le debug local
    stream = print_stream(df)
    # Ecrire les résultats dans le chat_ban
    kafka_stream = kafka_output(df)

    stream.awaitTermination()
    kafka_stream.awaitTermination()


if __name__ == "__main__":
    main()
