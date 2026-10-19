import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(11, 6.2)); ax.set_xlim(0, 110); ax.set_ylim(0, 62); ax.axis("off")

def box(x, y, w, h, txt, fc, ec="#333", fs=9, bold=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4", fc=fc, ec=ec, lw=1.2))
    ax.text(x + w/2, y + h/2, txt, ha="center", va="center", fontsize=fs,
            fontweight="bold" if bold else "normal")

def arrow(x1, y1, x2, y2, c="#444"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, color=c, lw=1.4))

ax.text(55, 59, "Arquitectura de Apache Kafka", ha="center", fontsize=14, fontweight="bold")

# Productores
for i, t in enumerate(["Producer 1\n(generador de datos)", "Producer 2\n(app / sensor)"]):
    box(2, 38 - i*16, 20, 10, t, "#cfe8ff", fs=8.5)
ax.text(12, 52, "PRODUCERS", ha="center", fontweight="bold", color="#1f4e79")

# Cluster
ax.add_patch(FancyBboxPatch((32, 6), 45, 48, boxstyle="round,pad=0.5", fc="#f6f6f6", ec="#555", lw=1.8, ls="--"))
ax.text(54.5, 51.5, "CLUSTER KAFKA", ha="center", fontweight="bold", color="#444")
brokers = [("Broker 1", 34, "P0 (líder)\nP2 (réplica)"), ("Broker 2", 47, "P1 (líder)\nP0 (réplica)"), ("Broker 3", 60, "P2 (líder)\nP1 (réplica)")]
for name, x, parts in brokers:
    box(x, 14, 12, 30, "", "#fff2cc")
    ax.text(x+6, 41, name, ha="center", fontweight="bold", fontsize=9)
    ax.text(x+6, 28, parts, ha="center", va="center", fontsize=8)
ax.text(54.5, 8.5, "Topic: casos_covid (3 particiones, replicación 2)", ha="center", fontsize=9, style="italic")

# Consumidores
ax.text(97, 52, "CONSUMER GROUP", ha="center", fontweight="bold", color="#375623")
for i, t in enumerate(["Consumer A\n(Spark Streaming)\n← P0", "Consumer B\n(Spark Streaming)\n← P1, P2"]):
    box(86, 36 - i*17, 20, 12, t, "#d9ead3", fs=8)

for y in (43, 27):
    arrow(22, y, 33, 29)
arrow(77, 29, 85, 42); arrow(77, 29, 85, 25)
ax.text(27, 20, "escriben\n(send)", ha="center", fontsize=8, color="#444")
ax.text(82, 19, "leen por offset\n(poll)", ha="center", fontsize=8, color="#444")
ax.text(54.5, 2, "Cada partición es un log ordenado e inmutable; cada mensaje tiene un offset. "
        "Una partición la lee un solo consumer del grupo.", ha="center", fontsize=7.5, color="#333")
plt.savefig("docs/diagrama_kafka.png", dpi=170, bbox_inches="tight"); print("ok")
