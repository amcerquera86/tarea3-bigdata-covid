# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) | Procesamiento en BATCH con Apache Spark (PySpark)

Problema: ¿Cómo se distribuyen los casos positivos de COVID-19 en Colombia por
territorio, sexo, edad, gravedad y tiempo, y qué grupos presentan mayor letalidad?

Fuente de datos (original): Datos Abiertos Colombia - INS
    "Casos positivos de COVID-19 en Colombia"  (id: gt2j-8ykr)
    https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr

Uso:
    python src/batch_covid_spark.py                    # 300.000 registros (rápido)
    python src/batch_covid_spark.py --max-rows 2000000 # más volumen
    python src/batch_covid_spark.py --max-rows 0       # TODO el dataset (varios millones)
    python src/batch_covid_spark.py --csv ruta.csv     # usar un CSV ya descargado
"""
import argparse
import os
import urllib.request

import matplotlib
matplotlib.use("Agg")  # backend sin interfaz gráfica (sirve en servidores/Colab)
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

API_URL = "https://www.datos.gov.co/resource/gt2j-8ykr.csv"
PAGE = 50000  # filas por petición a la API Socrata
DATA_DIR = "data"
FIG_DIR = "figures"


# ----------------------------------------------------------------------------
# 1. CARGA DESDE LA FUENTE ORIGINAL
# ----------------------------------------------------------------------------
def descargar_dataset(max_rows: int, destino: str) -> str:
    """Descarga el CSV desde la API de datos.gov.co paginando (max_rows=0 => todo)."""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    escritas, offset, primera = 0, 0, True
    with open(destino, "wb") as out:
        while True:
            lim = PAGE if max_rows == 0 else min(PAGE, max_rows - escritas)
            if lim <= 0:
                break
            url = f"{API_URL}?$limit={lim}&$offset={offset}&$order=:id"
            with urllib.request.urlopen(url, timeout=120) as r:
                contenido = r.read().decode("utf-8")
            lineas = contenido.splitlines()
            if len(lineas) <= 1:           # solo encabezado => no hay más datos
                break
            if primera:
                out.write((contenido + "\n").encode("utf-8"))
                primera = False
            else:
                out.write(("\n".join(lineas[1:]) + "\n").encode("utf-8"))
            n = len(lineas) - 1
            escritas += n
            offset += n
            print(f"  descargados {escritas:,} registros...")
            if n < lim:
                break
    return destino


# ----------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN (DataFrames)
# ----------------------------------------------------------------------------
def limpiar(df):
    # Normaliza nombres de columnas (minúsculas, sin espacios)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

    # Fechas ISO "2020-03-06T00:00:00.000" -> DateType
    for c in ["fecha_reporte_web", "fecha_de_notificaci_n", "fecha_inicio_sintomas",
              "fecha_muerte", "fecha_diagnostico", "fecha_recuperado"]:
        if c in df.columns:
            df = df.withColumn(c, F.to_date(F.substring(F.col(c), 1, 10)))

    # Edad en años (unidad_medida: 1=años, 2=meses, 3=días)
    df = df.withColumn("edad", F.col("edad").cast("double"))
    df = df.withColumn(
        "edad_anios",
        F.when(F.col("unidad_medida") == 1, F.col("edad"))
         .when(F.col("unidad_medida") == 2, F.col("edad") / 12)
         .when(F.col("unidad_medida") == 3, F.col("edad") / 365)
         .otherwise(F.col("edad")))

    # Estandarizar texto
    df = df.withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
    df = df.withColumn("estado", F.initcap(F.trim(F.col("estado"))))
    df = df.withColumn("fuente_tipo_contagio",
                       F.initcap(F.trim(F.col("fuente_tipo_contagio"))))

    # Eliminar registros inválidos y duplicados
    df = df.filter(F.col("edad_anios").isNotNull() & (F.col("edad_anios") >= 0)
                   & (F.col("edad_anios") <= 110))
    df = df.filter(F.col("sexo").isin("F", "M"))
    df = df.dropna(subset=["departamento_nom"]).dropDuplicates()

    # Variables derivadas
    df = df.withColumn(
        "grupo_edad",
        F.when(F.col("edad_anios") < 10, "00-09")
         .when(F.col("edad_anios") < 20, "10-19")
         .when(F.col("edad_anios") < 40, "20-39")
         .when(F.col("edad_anios") < 60, "40-59")
         .when(F.col("edad_anios") < 80, "60-79")
         .otherwise("80+"))
    df = df.withColumn("fallecido",
                       F.when((F.col("estado") == "Fallecido")
                              | F.col("fecha_muerte").isNotNull(), 1).otherwise(0))
    df = df.withColumn("mes", F.date_format(F.col("fecha_reporte_web"), "yyyy-MM"))
    return df


# ----------------------------------------------------------------------------
# 3. DEMOSTRACIÓN CON RDDs (transformaciones y acciones)
# ----------------------------------------------------------------------------
def demo_rdd(df):
    print("\n=== Demostración con RDDs ===")
    rdd = df.select("departamento_nom", "sexo", "fallecido", "edad_anios").rdd

    # map: (departamento, 1)    reduceByKey: suma por departamento
    casos_depto = (rdd.map(lambda r: (r["departamento_nom"], 1))
                      .reduceByKey(lambda a, b: a + b))
    print("Top 5 departamentos (map + reduceByKey + take):")
    for d, n in casos_depto.sortBy(lambda x: -x[1]).take(5):
        print(f"   {d}: {n:,}")

    # filter + count: fallecidos
    n_fall = rdd.filter(lambda r: r["fallecido"] == 1).count()
    print(f"Fallecidos (filter + count): {n_fall:,}")

    # flatMap: genera varias claves por registro (sexo y 'TOTAL')
    claves = rdd.flatMap(lambda r: [("sexo_" + r["sexo"], 1), ("TOTAL", 1)]) \
                .reduceByKey(lambda a, b: a + b).collect()
    print("flatMap + reduceByKey + collect:", sorted(claves))

    # groupByKey: edades por sexo -> promedio (menos eficiente que reduceByKey)
    prom = (rdd.map(lambda r: (r["sexo"], r["edad_anios"]))
               .groupByKey()
               .mapValues(lambda v: sum(v) / len(v))
               .collect())
    print("Edad promedio por sexo (groupByKey):", [(s, round(m, 1)) for s, m in prom])

    # reduce: edad máxima
    maxima = rdd.map(lambda r: r["edad_anios"]).reduce(max)
    print(f"Edad máxima (reduce): {maxima}")


# ----------------------------------------------------------------------------
# 4. ANÁLISIS EXPLORATORIO (EDA) + VISUALIZACIÓN
# ----------------------------------------------------------------------------
def barras(pdf, x, y, titulo, archivo, horizontal=False, color="#1f77b4"):
    plt.figure(figsize=(9, 5))
    if horizontal:
        plt.barh(pdf[x], pdf[y], color=color)
        plt.gca().invert_yaxis()
    else:
        plt.bar(pdf[x], pdf[y], color=color)
        plt.xticks(rotation=45, ha="right")
    plt.title(titulo)
    plt.tight_layout()
    ruta = os.path.join(FIG_DIR, archivo)
    plt.savefig(ruta, dpi=130)
    plt.close()
    print("  figura guardada:", ruta)


def eda(df):
    os.makedirs(FIG_DIR, exist_ok=True)
    df.cache()
    total = df.count()
    print(f"\n=== EDA === Registros limpios: {total:,}")
    df.printSchema()
    df.describe("edad_anios").show()

    # 4.1 Casos por departamento (Top 10)
    top_dep = (df.groupBy("departamento_nom").count()
                 .orderBy(F.desc("count")).limit(10))
    top_dep.show(truncate=False)
    barras(top_dep.toPandas(), "departamento_nom", "count",
           "Top 10 departamentos por casos positivos", "01_casos_departamento.png",
           horizontal=True)

    # 4.2 Casos por sexo
    sexo = df.groupBy("sexo").count().toPandas()
    plt.figure(figsize=(5, 5))
    plt.pie(sexo["count"], labels=sexo["sexo"], autopct="%1.1f%%",
            colors=["#e377c2", "#1f77b4"])
    plt.title("Distribución de casos por sexo")
    plt.savefig(os.path.join(FIG_DIR, "02_casos_sexo.png"), dpi=130)
    plt.close()

    # 4.3 Letalidad por grupo de edad
    letalidad = (df.groupBy("grupo_edad")
                   .agg(F.count("*").alias("casos"),
                        F.sum("fallecido").alias("fallecidos"))
                   .withColumn("letalidad_pct",
                               F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
                   .orderBy("grupo_edad"))
    letalidad.show()
    barras(letalidad.toPandas(), "grupo_edad", "letalidad_pct",
           "Letalidad (%) por grupo de edad", "03_letalidad_edad.png", color="#d62728")

    # 4.4 Serie temporal mensual
    serie = (df.groupBy("mes").count().orderBy("mes")
               .filter(F.col("mes").isNotNull()).toPandas())
    plt.figure(figsize=(10, 4.5))
    plt.plot(serie["mes"], serie["count"], marker="o", color="#2ca02c")
    plt.xticks(rotation=90, fontsize=7)
    plt.title("Casos reportados por mes")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "04_serie_mensual.png"), dpi=130)
    plt.close()

    # 4.5 Gravedad y tipo de contagio
    estado = df.groupBy("estado").count().orderBy(F.desc("count"))
    estado.show()
    barras(estado.toPandas(), "estado", "count", "Casos según estado/gravedad",
           "05_estado.png", color="#ff7f0e")

    # 4.6 Consulta con Spark SQL (letalidad por departamento, mínimo 1.000 casos)
    df.createOrReplaceTempView("casos")
    spark = df.sparkSession
    top_let = spark.sql("""
        SELECT departamento_nom, COUNT(*) AS casos, SUM(fallecido) AS fallecidos,
               ROUND(100*SUM(fallecido)/COUNT(*), 2) AS letalidad_pct
        FROM casos GROUP BY departamento_nom HAVING COUNT(*) >= 1000
        ORDER BY letalidad_pct DESC LIMIT 10""")
    top_let.show(truncate=False)
    barras(top_let.toPandas(), "departamento_nom", "letalidad_pct",
           "Top 10 departamentos por letalidad (%)", "06_letalidad_departamento.png",
           horizontal=True, color="#9467bd")

    # Resultado final guardado en formato columnar Parquet
    letalidad.write.mode("overwrite").parquet("data/resultado_letalidad_parquet")


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rows", type=int, default=300000,
                    help="Registros a descargar (0 = todos)")
    ap.add_argument("--csv", help="Ruta a un CSV ya descargado")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("Tarea3_Batch_COVID_Colombia")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ruta = args.csv or os.path.join(DATA_DIR, "covid_colombia.csv")
    if not os.path.exists(ruta):
        print("Descargando desde datos.gov.co ...")
        descargar_dataset(args.max_rows, ruta)

    crudo = spark.read.csv(ruta, header=True, inferSchema=False)   # todo string
    print(f"Registros crudos: {crudo.count():,} | Columnas: {len(crudo.columns)}")

    df = limpiar(crudo)
    demo_rdd(df)
    eda(df)
    spark.stop()
    print("\nListo. Revisa la carpeta 'figures/'.")


if __name__ == "__main__":
    main()
