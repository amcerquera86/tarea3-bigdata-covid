# -*- coding: utf-8 -*-
"""Generador de datos: simula la llegada en tiempo real de nuevos casos COVID-19
al topic 'casos_covid' de Kafka (un evento JSON por mensaje).

    pip install kafka-python
    python src/kafka_producer.py --rate 5      # ~5 eventos por segundo
"""
import argparse, json, random, time
from datetime import datetime
from kafka import KafkaProducer

DEPARTAMENTOS = {"BOGOTA": 0.28, "ANTIOQUIA": 0.14, "VALLE": 0.08, "CUNDINAMARCA": 0.05,
                 "ATLANTICO": 0.05, "SANTANDER": 0.04, "BOLIVAR": 0.03,
                 "NORTE SANTANDER": 0.03, "CORDOBA": 0.02, "TOLIMA": 0.02, "OTROS": 0.26}
ESTADOS = {"Leve": 0.93, "Moderado": 0.03, "Grave": 0.01, "Fallecido": 0.03}
CONTAGIO = ["Comunitaria", "Relacionado", "Importado", "En estudio"]


def elegir(dic):
    return random.choices(list(dic), weights=list(dic.values()))[0]


def evento():
    return {"id_caso": random.randint(1, 10**8),
            "departamento": elegir(DEPARTAMENTOS),
            "sexo": random.choice(["F", "M"]),
            "edad": max(0, min(100, int(random.gauss(40, 18)))),
            "estado": elegir(ESTADOS),
            "fuente_tipo_contagio": random.choice(CONTAGIO),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--topic", default="casos_covid")
    ap.add_argument("--rate", type=float, default=5, help="eventos por segundo")
    a = ap.parse_args()
    prod = KafkaProducer(bootstrap_servers=a.broker,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print(f"Enviando eventos a '{a.topic}' en {a.broker} (Ctrl+C para detener)")
    try:
        while True:
            e = evento()
            prod.send(a.topic, e)
            print("enviado:", e)
            time.sleep(1.0 / a.rate)
    except KeyboardInterrupt:
        prod.flush(); prod.close()
