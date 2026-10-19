# -*- coding: utf-8 -*-
"""Consumidor Spark Structured Streaming <- Kafka (topic 'casos_covid').

Procesamiento en tiempo real:
  1) Casos por departamento en ventanas de 30 s (cada 10 s).
  2) Edad promedio y número de fallecidos por ventana.
  3) Visualización: tablas en consola + gráfico PNG que se actualiza en cada micro-lote.

Ejecutar (ajusta la versión del conector a tu versión de Spark/Scala):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      src/spark_streaming_consumer.py
"""
import argparse, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

ESQUEMA = StructType([
    StructField("id_caso", IntegerType()), StructField("departamento", StringType()),
    StructField("sexo", StringType()), StructField("edad", IntegerType()),
    StructField("estado", StringType()), StructField("fuente_tipo_contagio", StringType()),
    StructField("timestamp", StringType())])

os.makedirs("figures", exist_ok=True)


def graficar(batch_df, batch_id):
    """Se ejecuta por cada micro-lote: imprime y grafica el acumulado por departamento."""
    pdf = (batch_df.groupBy("departamento").agg(F.sum("casos").alias("casos"))
                   .orderBy(F.desc("casos")).toPandas())
    if pdf.empty:
        return
    print(f"\n--- micro-lote {batch_id} ---")
    print(pdf.to_string(index=False))
    plt.figure(figsize=(9, 4.5))
    plt.bar(pdf["departamento"], pdf["casos"], color="#1f77b4")
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Casos en tiempo real por departamento (lote {batch_id})")
    plt.tight_layout()
    plt.savefig("figures/streaming_departamentos.png", dpi=110)
    plt.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--topic", default="casos_covid")
    a = ap.parse_args()

    spark = SparkSession.builder.appName("Tarea3_Streaming_Kafka").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    crudo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", a.broker)
             .option("subscribe", a.topic)
             .option("startingOffsets", "latest").load())

    eventos = (crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e"))
                    .select("e.*")
                    .withColumn("ts", F.to_timestamp("timestamp")))

    ventana = F.window("ts", "30 seconds", "10 seconds")

    # (1) Conteo por departamento y ventana
    por_depto = (eventos.withWatermark("ts", "1 minute")
                 .groupBy(ventana.alias("ventana"), "departamento")
                 .agg(F.count("*").alias("casos")))

    # (2) Estadísticas generales por ventana
    stats = (eventos.withWatermark("ts", "1 minute")
             .groupBy(ventana.alias("ventana"))
             .agg(F.count("*").alias("total_casos"),
                  F.round(F.avg("edad"), 1).alias("edad_promedio"),
                  F.sum(F.when(F.col("estado") == "Fallecido", 1).otherwise(0))
                   .alias("fallecidos")))

    q1 = (por_depto.writeStream.outputMode("update").foreachBatch(graficar)
          .option("checkpointLocation", "/tmp/chk_depto").start())
    q2 = (stats.writeStream.outputMode("update").format("console")
          .option("truncate", "false").option("checkpointLocation", "/tmp/chk_stats").start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
