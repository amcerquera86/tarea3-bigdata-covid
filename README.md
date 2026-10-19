# Tarea 3 – Procesamiento de Datos con Apache Spark

Curso **Big Data (202016911)** – UNAD, Ingeniería de Sistemas.
Autor: **[tu nombre]** · Grupo: **[número]**

## 1. Problema

¿Cómo se distribuyen los casos positivos de COVID-19 en Colombia por departamento,
sexo, edad, gravedad y tiempo, y qué grupos presentan mayor letalidad?

## 2. Conjunto de datos

**Casos positivos de COVID-19 en Colombia** – Instituto Nacional de Salud, Datos Abiertos
Colombia (id `gt2j-8ykr`):
https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr

Un registro por caso (millones de filas). El script lo descarga de la API del portal, no
hace falta bajarlo a mano.

## 3. Solución (procesamiento batch con PySpark)

El script `src/batch_covid_spark.py` sigue estos pasos:

1. **Carga** desde la fuente original (API Socrata, paginada) y lectura con `spark.read.csv`.
2. **Limpieza y transformación** con DataFrames: normaliza nombres de columnas, convierte
   fechas, unifica la edad a años, estandariza texto, elimina nulos, valores inválidos y
   duplicados, y crea variables derivadas (`grupo_edad`, `fallecido`, `mes`).
3. **Demostración con RDDs:** `map`, `filter`, `flatMap`, `groupByKey`, `reduceByKey`,
   `collect`, `count`, `take` y `reduce`.
4. **EDA con DataFrames y Spark SQL:** casos por departamento, sexo, letalidad por grupo de
   edad y por departamento, serie temporal mensual y distribución por gravedad.
5. **Visualización:** gráficas con matplotlib en `figures/`; el resultado final se guarda en
   formato Parquet.

## 4. Estructura

```
src/batch_covid_spark.py          Procesamiento batch (entregable principal)
src/kafka_producer.py             Generador de datos para el topic de Kafka (tiempo real)
src/spark_streaming_consumer.py   Spark Structured Streaming que consume de Kafka
docs/diagrama_kafka.png           Diagrama de arquitectura de Kafka
figures/                          Gráficas generadas
requirements.txt                  Dependencias de Python
```

## 5. Requisitos

- Python 3.9+ y Java 11 o 17
- Apache Spark 3.5.x
- `pip install -r requirements.txt` (pyspark, matplotlib, pandas, kafka-python)

## 6. Cómo ejecutar el batch

```bash
git clone [URL_DE_TU_REPOSITORIO]
cd [carpeta]
pip install -r requirements.txt

# Prueba rápida (100.000 registros). Recomendado en máquinas con poca RAM
spark-submit --driver-memory 1g src/batch_covid_spark.py --max-rows 100000

# Más volumen
spark-submit --driver-memory 2g src/batch_covid_spark.py --max-rows 1000000

# Todo el dataset (necesita bastante memoria y disco)
spark-submit --driver-memory 4g src/batch_covid_spark.py --max-rows 0

# Usar un CSV ya descargado
spark-submit src/batch_covid_spark.py --csv ruta/al/archivo.csv
```

Si cambias `--max-rows`, borra antes `data/covid_colombia.csv`; si no, no se vuelve a
descargar. La salida es texto en consola, las gráficas en `figures/` y el resultado en
`data/resultado_letalidad_parquet`.

## 7. Tiempo real (Kafka + Spark Streaming)

Se sigue el Anexo 3 del curso (ZooKeeper, Kafka, topic, productor y consumidor).
Resumen: iniciar ZooKeeper y Kafka, crear el topic y, en terminales distintas, correr
`python3 src/kafka_producer.py` y
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_consumer.py`.

## 8. Resultados

[Pega aquí 2 o 3 gráficas de `figures/` y comenta los hallazgos con las cifras reales
que obtengas al ejecutar el código.]

## 9. Autor y licencia

[Tu nombre] – Trabajo académico, UNAD. Datos: Instituto Nacional de Salud (datos abiertos).
