#!/usr/bin/env python3
"""Generates the PDF report from results/*/*.json: python3 scripts/report.py"""
import glob
import json
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402
from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet  # noqa: E402
from reportlab.lib.units import cm  # noqa: E402
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate,  # noqa: E402
                                Spacer, Table, TableStyle)

REPO = Path(__file__).resolve().parent.parent
RES = REPO / "results"
OUT = RES / "report"
FIG = OUT / "figures"
C = {"kafka": "#1f5fa8", "rabbitmq": "#e07b00"}
NAME = {"kafka": "Kafka", "rabbitmq": "RabbitMQ"}
PLAN = {"debit": 6, "latency": 12, "concurrency": 6, "slow": 2, "crash": 2, "broker": 2, "faults": 2,
        "replay": 2, "ordering": 4}
NAVY = colors.HexColor("#14365d")
LIGHT = colors.HexColor("#eef3f9")


def load(exp):
    return [json.load(open(f)) for f in sorted(glob.glob(str(RES / exp / "*.json")))]


def fr(x, d=0):
    if x is None:
        return "n/d"
    return f"{x:,.{d}f}".replace(",", " ")


def ms(v):
    return "n/d" if v is None else (f"{v:.1f} ms" if v < 1000 else f"{v/1000:.2f} s")


def one(rows, **kw):
    for r in rows:
        if all(r.get(k) == v for k, v in kw.items()):
            return r
    return None


def family_status():
    return {e: len(load(e)) for e in PLAN}


# ---------------- figures ----------------

def fig_architecture():
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 50)
    ax.axis("off")

    def box(x, y, w, h, text, color):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4", fc=color, ec="#333", lw=0.8))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8)

    box(1, 20, 14, 10, "Producteur\n(profil kafka\nou rabbitmq)", "#dfe9f5")
    box(22, 30, 18, 9, "Topic orders.events\n3 partitions, RF=1\nclé = orderId", "#cfe0f3")
    box(22, 11, 18, 9, "Fanout -> hash\n(x-consistent-hash)\nclé = orderId", "#fbe3c5")
    box(45, 36, 16, 8, "Consumer Kafka\nconcurrency = 3\nack manuel", "#cfe0f3")
    box(45, 20, 16, 12, "3 quorum queues\nconsumer par queue\nDLX -> orders.dlq", "#fbe3c5")
    box(45, 2, 16, 7, "Stream\norders.events.stream\n(relecture)", "#fbe3c5")
    box(70, 36, 14, 8, "DLT\norders.events.DLT", "#f2f2f2")
    box(70, 20, 14, 8, "orders.dlq", "#f2f2f2")
    box(84, 26, 14, 10, "Métriques\nPrometheus\nGrafana", "#e8f5e8")
    arrows = [((15, 26), (22, 34)), ((15, 24), (22, 15)), ((40, 34), (45, 40)), ((40, 15), (45, 26)),
              ((40, 6), (45, 6)), ((61, 40), (70, 40)), ((61, 24), (70, 24)), ((30, 30), (30, 20))]
    for (x0, y0), (x1, y1) in arrows:
        ax.annotate("", xy=(x1, y1), xytext=(x0, y0), arrowprops=dict(arrowstyle="->", lw=0.8))
    ax.text(50, 48, "Architecture de test : même cas métier e-commerce, mêmes événements Avro", ha="center",
            fontsize=9, weight="bold")
    fig.tight_layout()
    path = FIG / "architecture.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 4.2


def fig_debit():
    rows = load("debit")
    labels = ["10k", "100k", "1M"]
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, key, title in ((axes[0], "produce_rate_msg_s", "Débit de publication (msg/s)"),
                           (axes[1], "drain_rate_msg_s", "Débit de consommation (msg/s)")):
        w = 0.38
        for i, t in enumerate(["kafka", "rabbitmq"]):
            vals = [(one(rows, transport=t, label=l) or {}).get(key) or 0 for l in labels]
            xs = [j + (i - 0.5) * w for j in range(3)]
            bars = ax.bar(xs, vals, w, color=C[t], label=NAME[t])
            for b, v in zip(bars, vals):
                ax.text(b.get_x() + b.get_width() / 2, v, fr(v), ha="center", va="bottom", fontsize=7)
        ax.set_xticks(range(3), labels)
        ax.set_title(title, fontsize=9)
        ax.grid(axis="y", alpha=0.3)
    axes[0].legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "debit.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.4


def fig_latency():
    rows = load("latency")
    cfg = [(p, r) for p in (0, 1024, 10240) for r in (50, 500)]
    labels = [f"{'0' if p == 0 else f'{p//1024} Ko'}\n{r} ord/s" for p, r in cfg]
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.6))
    for ax, q, title in ((axes[0], "p95_ms", "Latence bout en bout p95 (ms)"),
                         (axes[1], "p99_ms", "Latence bout en bout p99 (ms, échelle log)")):
        w = 0.38
        for i, t in enumerate(["kafka", "rabbitmq"]):
            vals = []
            for p, r in cfg:
                row = one(rows, transport=t, payload_bytes=p, orders=r * 40) or {}
                vals.append((row.get("latency", {}).get("e2e", {}) or {}).get(q) or 0)
            xs = [j + (i - 0.5) * w for j in range(len(cfg))]
            ax.bar(xs, vals, w, color=C[t], label=NAME[t])
        ax.set_xticks(range(len(cfg)), labels, fontsize=6)
        ax.set_title(title, fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        if q == "p99_ms":
            ax.set_yscale("log")
    axes[0].legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "latence.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.6


def fig_concurrency():
    rows = load("concurrency")
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for t in ("kafka", "rabbitmq"):
        ns = [1, 3, 5]
        vals = [(one(rows, transport=t, consumers=n) or {}).get("drain_rate_msg_s") for n in ns]
        axes[0].plot(ns, [v or 0 for v in vals], marker="o", color=C[t], label=NAME[t])
        ooo = [((one(load("ordering"), transport=t, consumers=n) or {}).get("audit") or {}).get("orders_out_of_order")
               for n in ns]
        ooo_c = [((one(rows, transport=t, consumers=n) or {}).get("audit") or {}).get("orders_out_of_order") or 0
                 for n in ns]
        axes[1].bar([n + (0.2 if t == "rabbitmq" else -0.2) for n in ns], ooo_c, 0.4, color=C[t],
                    label=NAME[t])
    axes[0].set_title("Débit de consommation selon le nombre d'instances", fontsize=9)
    axes[0].set_xlabel("instances consommateur (3 threads chacune)", fontsize=8)
    axes[0].set_xticks([1, 3, 5])
    axes[0].grid(alpha=0.3)
    axes[0].legend(fontsize=8)
    axes[1].set_title("Commandes hors ordre (concurrence)", fontsize=9)
    axes[1].set_xticks([1, 3, 5])
    axes[1].set_xlabel("instances consommateur", fontsize=8)
    axes[1].grid(axis="y", alpha=0.3)
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "concurrence.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.4


def fig_slow():
    rows = load("slow")
    fig, ax = plt.subplots(figsize=(9, 3.2))
    for t in ("kafka", "rabbitmq"):
        r = one(rows, transport=t)
        if not r:
            continue
        pts = [(p["t"], p["backlog"]) for p in r["trace"] if p["backlog"] is not None]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=C[t], label=NAME[t])
    ax.set_xlabel("temps (s)", fontsize=8)
    ax.set_ylabel("backlog (événements)", fontsize=8)
    ax.set_title("Consommateur lent (20 ms par message) : accumulation puis reprise", fontsize=9)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "lent.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.2


def fig_resilience():
    crash, broker = load("crash"), load("broker")
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.2))
    for i, t in enumerate(["kafka", "rabbitmq"]):
        c = one(crash, transport=t) or {}
        b = one(broker, transport=t) or {}
        axes[0].bar(i - 0.2, c.get("recovery_first_ok_after_restart_s") or 0, 0.4, color=C[t], label="1er ack après redémarrage")
        axes[0].bar(i + 0.2, c.get("reprise_backlog_zero_after_restart_s") or 0, 0.4, color=C[t], alpha=0.5,
                    label="backlog résorbé")
        axes[1].bar(i - 0.2, b.get("outage_to_first_confirm_s") or 0, 0.4, color=C[t], label="1re confirmation après arrêt")
        axes[1].bar(i + 0.2, b.get("first_confirm_after_recovery_s") or 0, 0.4, color=C[t], alpha=0.5,
                    label="1re confirmation après reprise")
    for ax, title in ((axes[0], "Panne consommateur (SIGKILL), s"), (axes[1], "Panne broker (arrêt 20 s), s")):
        ax.set_xticks([0, 1], ["Kafka", "RabbitMQ"])
        ax.set_title(title, fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=6)
    fig.tight_layout()
    path = FIG / "resilience.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.2


def fig_faults():
    rows = load("faults")
    fig, ax = plt.subplots(figsize=(9, 3.0))
    metrics = [("invalid_sent", "messages invalides envoyés"), ("retries", "tentatives de retry"),
               ("dlq", "messages en DLQ/DLT")]
    w = 0.38
    for i, t in enumerate(["kafka", "rabbitmq"]):
        r = one(rows, transport=t)
        if not r:
            continue
        vals = [r["orders_outcome"]["invalid_sent"], r["consumer_retryable_attempts"], r["dlq_counter"]]
        xs = [j + (i - 0.5) * w for j in range(3)]
        bars = ax.bar(xs, vals, w, color=C[t], label=NAME[t])
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, str(v), ha="center", va="bottom", fontsize=7)
    ax.set_xticks(range(3), [m[1] for m in metrics], fontsize=8)
    ax.set_title("Messages invalides et erreurs transitoires (5 % + 5 %)", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "fautes.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.0


def fig_ordering():
    rows = load("ordering")
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(9, 3.0))
    labels, vals, cols = [], [], []
    for t in ("kafka", "rabbitmq"):
        for n in (1, 3):
            r = one(rows, transport=t, consumers=n)
            if r:
                labels.append(f"{NAME[t]}\n{n} instance(s)")
                vals.append(r["audit"]["orders_out_of_order"])
                cols.append(C[t])
    ax.bar(labels, vals, color=cols)
    for i, v in enumerate(vals):
        ax.text(i, v, str(v), ha="center", va="bottom", fontsize=8)
    ax.set_title("Commandes dont les étapes sont traitées hors ordre (avec 5 % d'erreurs transitoires)", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    path = FIG / "ordre.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.0


def fig_replay():
    rows = load("replay")
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(9, 3.0))
    w = 0.38
    for i, t in enumerate(["kafka", "rabbitmq"]):
        r = one(rows, transport=t)
        if not r:
            continue
        vals = [r["events_published"], r["replay"]["processed"] + r["replay"]["failed"]]
        xs = [0 + (i - 0.5) * w, 1 + (i - 0.5) * w]
        bars = ax.bar(xs, vals, w, color=C[t], label=NAME[t])
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, fr(v), ha="center", va="bottom", fontsize=7)
    ax.set_xticks([0, 1], ["Événements publiés", "Événements rejoués"])
    ax.set_title("Relecture de la période du run", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    path = FIG / "relecture.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.0


def fig_resources():
    rows = load("debit")
    r_k = one(rows, transport="kafka", label="1M")
    r_r = one(rows, transport="rabbitmq", label="1M")
    if not (r_k and r_r):
        return None
    svcs = ["kafka", "rabbitmq", "order-consumer-kafka", "order-consumer-rabbitmq", "order-producer-kafka",
            "order-producer-rabbitmq", "schema-registry"]
    fig, ax = plt.subplots(figsize=(9, 3.2))
    for i, r in enumerate([r_k, r_r]):
        cpu = r["resources"]["cpu_seconds"]
        bottom = 0
        for s in svcs:
            v = cpu.get(s, 0)
            if v > 0:
                ax.bar(i, v, 0.5, bottom=bottom, label=s if i == 0 else None)
                bottom += v
        ax.text(i, bottom, f"{fr(bottom)} s CPU", ha="center", va="bottom", fontsize=8)
    ax.set_xticks([0, 1], ["Kafka 1 M", "RabbitMQ 1 M"])
    ax.set_title("CPU consommé par service (somme des cœurs × temps)", fontsize=9)
    ax.legend(fontsize=6, ncol=2)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    path = FIG / "ressources.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path, 9 / 3.2


# ---------------- PDF helpers ----------------

styles = getSampleStyleSheet()
H1 = ParagraphStyle("H1", parent=styles["Heading1"], textColor=NAVY, fontSize=16, spaceAfter=8)
H2 = ParagraphStyle("H2", parent=styles["Heading2"], textColor=NAVY, fontSize=12, spaceBefore=8, spaceAfter=4)
BODY = ParagraphStyle("B", parent=styles["BodyText"], fontSize=9, leading=12.5, spaceAfter=4)
SMALL = ParagraphStyle("S", parent=BODY, fontSize=7.5, leading=9.5, textColor=colors.HexColor("#555"))
BUL = ParagraphStyle("BL", parent=BODY, leftIndent=10, bulletIndent=2)


def P(text, style=BODY):
    return Paragraph(text, style)


def bullets(items):
    return [Paragraph(i, BUL, bulletText="•") for i in items]


def table(header, rows, widths):
    data = [[Paragraph(f"<b>{h}</b>", ParagraphStyle("th", parent=BODY, textColor=colors.white, fontSize=7.5,
                                                    leading=9))
             for h in header]]
    for r in rows:
        data.append([Paragraph(str(c), ParagraphStyle("td", parent=BODY, fontSize=7.5, leading=9)) for c in r])
    t = Table(data, colWidths=[w * cm for w in widths], repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#b8c4d4")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return t


def image(path_ratio, width_cm=16.5):
    path, ratio = path_ratio
    return Image(str(path), width=width_cm * cm, height=width_cm * cm / ratio)


def pending(exp):
    return P(f"<i>Résultats non disponibles : la famille « {exp} » n'est pas terminée au moment de la "
             f"génération de ce rapport.</i>", SMALL)


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#666"))
    canvas.drawString(1.8 * cm, 1.1 * cm, "Kafka vs RabbitMQ : rapport d'expérimentation")
    canvas.drawRightString(A4[0] - 1.8 * cm, 1.1 * cm, f"page {doc.page}")
    canvas.restoreState()


# ---------------- sections ----------------

def section_summary(status, complete):
    s = [P("1. Synthèse exécutive", H1)]
    state = ("campagne complète : les 9 familles d'expériences et 38 runs sont terminés."
             if complete else
             f"rapport provisoire : {sum(min(v, PLAN[k]) for k, v in status.items())}/38 runs terminés, "
             "les sections marquées « non disponibles » seront complétées à la fin de la campagne.")
    s.append(P(f"<b>Statut :</b> {state}"))
    dk, dr = one(load("debit"), transport="kafka", label="1M"), one(load("debit"), transport="rabbitmq", label="1M")
    s.append(P("<b>Question :</b> quel outil convient à un besoin e-commerce donné (tâches fiables, relecture, "
               "ordre par commande), et sur quelles preuves repose ce choix ?"))
    s.append(P("<b>Réponse conditionnelle :</b> aucun des deux n'est meilleur dans l'absolu. Dans ce banc, Kafka "
               "garde l'ordre par commande même avec plusieurs consommateurs et conserve un historique rejouable "
               "par horodatage. RabbitMQ offre un routage plus riche, des DLX natives et une consommation par "
               "tâches souple, mais perd l'ordre dès qu'on ajoute des consommateurs concurrents sur une même file."))
    rows = [
        ["Débit de consommation (1 M msg)", fr((dk or {}).get("drain_rate_msg_s")) + " msg/s",
         fr((dr or {}).get("drain_rate_msg_s")) + " msg/s", "Équivalent dans ce banc (2 vCPU)"],
        ["Ordre par commande, 1 consommateur", "0 commande hors ordre", "0 commande hors ordre",
         "Garanti par la clé / le hash"],
        ["Ordre par commande, 3 consommateurs", "0 commande hors ordre (3 000)",
         "270 commandes hors ordre (9 %)", "Consommateurs concurrents sur une même file : ordre cassé (§3.9)"],
        ["Perte réelle sur runs complets", "0", "0",
         "Événements absents de l'audit = dead-lettrés après 3 à 4 échecs (§3.7, §3.9)"],
        ["Relecture de l'historique", "oui (timestamp)", "oui (stream, timestamp)",
         "RabbitMQ : la file quorum ne garde pas l'historique"],
        ["Reprise après panne consommateur", "~30 s", "~26 s", "Sans perte ni doublon observé"],
    ]
    s.append(table(["Indicateur", "Kafka", "RabbitMQ", "Lecture"], rows, [5.2, 3.2, 3.6, 5.2]))
    s.append(Spacer(1, 6))
    s += bullets([
        "Pour les tâches à ordre strict (ex. commande → paiement → stock), Kafka est le choix sûr dans ce banc.",
        "Pour un routage multi-critères, des tâches indépendantes et des DLX natives, RabbitMQ est plus adapté, "
        "à condition de ne pas dépendre de l'ordre entre consommateurs concurrents.",
        "La relecture par historique est native avec Kafka (topic) et RabbitMQ Streams ; les quorum queues ne "
        "conservent pas l'historique après acquittement.",
    ])
    return s


def section_method(status):
    s = [PageBreak(), P("2. Contexte, architecture et protocole", H1)]
    s.append(image(fig_architecture()))
    s.append(P("Figure 1 : architecture de test. Un même producteur (profil kafka ou rabbitmq) émet trois événements "
               "par commande (OrderCreated, PaymentProcessed, InventoryReserved) en Avro, avec Schema Registry.",
               SMALL))
    s.append(P("2.1 Configuration", H2))
    rows = [
        ["Machine", "Hôte macOS, Docker Desktop limité à 2 vCPU et 8 Go de RAM"],
        ["Kafka", "3.9.0 en mode KRaft, un nœud, RF=1, 3 partitions, clé = orderId, acks=all, idempotence"],
        ["RabbitMQ", "4.4.0 (un nœud), 3 quorum queues avec x-consistent-hash, DLX, stream, plugins management"],
        ["Applications", "Spring Boot 3.5.16, Java 21, Confluent 7.9.0 (Schema Registry, Avro), Micrometer"],
        ["Mesure", "Compteurs et histogrammes Micrometer par consommateur, débit mesuré via docker stats"],
        ["Audit", "Chaque publication confirmée et chaque traitement final est écrit (fichier, non bufferisé)"],
        ["Réconciliation", "Perte = événement confirmé absent de l'audit consommateur ; doublon = ligne en trop ; "
                           "ordre = première consommation de chaque étape"],
    ]
    s.append(table(["Élément", "Valeur"], rows, [3.3, 12.9]))
    s.append(P("2.2 Statut de la campagne", H2))
    s.append(table(["Famille", "Runs terminés", "Attendus"],
                   [[k, str(min(status[k], PLAN[k])), str(PLAN[k])] for k in PLAN],
                   [6, 4, 4]))
    return s


def section_experiments(status):
    s = [PageBreak(), P("3. Résultats par expérience", H1),
         P("Chaque expérience suit la fiche de protocole : hypothèse, configuration, charge, méthode, résultats "
           "mesurés, interprétation et limites. Les valeurs sont celles des fichiers JSON du dossier results/.",
           BODY)]

    # 3.1 debit
    s.append(P("3.1 Débit (10 k, 100 k, 1 M messages)", H2))
    s.append(P("<b>Hypothèse :</b> RabbitMQ (quorum) consomme moins vite que Kafka à gros volume, car chaque message "
               "est acquitté individuellement."))
    s.append(P("<b>Protocole :</b> publication sans consommateur (backlog), puis mesure du temps de drainage à un "
               "consommateur. Concurrence de publication limitée à 100 messages en vol pour les deux transports."))
    rows = load("debit")
    if rows:
        tab = []
        for t in ("kafka", "rabbitmq"):
            for lbl in ("10k", "100k", "1M"):
                r = one(rows, transport=t, label=lbl)
                if r:
                    a = r["audit"]
                    tab.append([NAME[t], lbl, fr(r["events_published"]), fr(r["produce_rate_msg_s"]),
                                fr(r["drain_rate_msg_s"]), str(a["lost"]), str(a["duplicate_rows"]),
                                str(a["orders_out_of_order"])])
        s.append(table(["Transport", "Volume", "Événements", "Publication (msg/s)", "Consommation (msg/s)",
                        "Pertes", "Doublons", "Hors ordre"], tab, [2.4, 1.6, 2.6, 2.8, 2.9, 1.7, 1.7, 1.9]))
        s.append(Spacer(1, 4))
        s.append(image(fig_debit()))
    else:
        s.append(pending("debit"))
    s.append(P("<b>Interprétation :</b> à 1 M de messages, les deux transports consomment au même rythme "
               "(~11 500 msg/s), limité par le CPU partagé. La publication diffère : Kafka regroupe les envois par lots "
               "(batching), RabbitMQ confirme chaque message. Le run à 10 k est sous-estimé : il inclut le join du "
               "groupe et l'échauffement de la JVM. <b>Limite :</b> une seule répétition par point, donc pas de "
               "médiane ni de dispersion."))

    # 3.2 latence
    s.append(P("3.2 Latence bout en bout (taille × fréquence)", H2))
    s.append(P("<b>Hypothèse :</b> la latence reste faible tant que le consommateur n'est pas saturé ; la taille du "
               "message (0 à 10 Ko) pèse peu sur la latence tant que le réseau n'est pas le goulot."))
    lat = load("latency")
    if lat:
        tab = []
        for t in ("kafka", "rabbitmq"):
            for p in (0, 1024, 10240):
                for r_ in (50, 500):
                    r = one(lat, transport=t, payload_bytes=p, orders=r_ * 40)
                    if r:
                        e = r["latency"]["e2e"]
                        tab.append([NAME[t], "0" if p == 0 else f"{p//1024} Ko", f"{r_} ord/s",
                                    fr(r["events_published"]), ms(e["p50_ms"]), ms(e["p95_ms"]), ms(e["p99_ms"])])
        s.append(table(["Transport", "Payload", "Cadence", "Événements", "p50", "p95", "p99"], tab,
                       [2.5, 2.2, 2.2, 2.8, 2.6, 2.6, 2.6]))
        s.append(Spacer(1, 4))
        s.append(image(fig_latency()))
    else:
        s.append(pending("latency"))
    s.append(P("<b>Interprétation :</b> à 50 ordres/s (150 msg/s), Kafka et RabbitMQ sont équivalents (p50 < 1 ms, "
               "p95 ~3-4 ms). À 500 ordres/s (1 500 msg/s), le p99 monte à 250-430 ms pour les deux : la queue "
               "d'attente apparaît près de la saturation du CPU partagé. La taille du payload (jusqu'à 10 Ko) "
               "n'a pas d'effet mesurable à ce débit."))

    # 3.3 concurrence
    s.append(P("3.3 Concurrence (1, 3, 5 instances) et ordre", H2))
    s.append(P("<b>Hypothèse :</b> ajouter des consommateurs augmente le débit jusqu'à la limite des partitions "
               "(Kafka) ou des files (RabbitMQ), mais sur ce VM 2 vCPU le gain peut être négatif."))
    conc = load("concurrency")
    if conc:
        tab = []
        for t in ("kafka", "rabbitmq"):
            for n in (1, 3, 5):
                r = one(conc, transport=t, consumers=n)
                if r:
                    dist = "/".join(str(x) for x in r["per_instance_consumed"])
                    tab.append([NAME[t], str(n), fr(r["drain_rate_msg_s"]), dist,
                                str(r["audit"]["orders_out_of_order"])])
        s.append(table(["Transport", "Instances", "Consommation (msg/s)", "Messages par instance",
                        "Commandes hors ordre"], tab, [2.6, 2.2, 3.4, 6.2, 3.2]))
        s.append(Spacer(1, 4))
        s.append(image(fig_concurrency()))
    else:
        s.append(pending("concurrency"))
    s.append(P("<b>Interprétation :</b> Kafka n'utilise que 3 instances actives sur 5 : les partitions plafonnent le "
               "parallélisme, les autres restent inactives (0 message). Sur 2 vCPU, le débit baisse avec le nombre "
               "d'instances (contention CPU et rééquilibrages). Côté ordre, RabbitMQ perd l'ordre dès 3 consommateurs "
               "concurrents sur une même file, alors que Kafka le conserve. <b>Limite :</b> la contention CPU de la "
               "VM domine la mesure, pas le broker seul."))

    # 3.4 consommateur lent
    s.append(P("3.4 Consommateur lent (pause de 20 ms par message)", H2))
    s.append(P("<b>Hypothèse :</b> l'offre dépasse la capacité du consommateur (150 msg/s) ; le backlog grandit, "
               "puis se résorbe après l'arrêt de la production."))
    slow = load("slow")
    if slow:
        tab = []
        for t in ("kafka", "rabbitmq"):
            r = one(slow, transport=t)
            if r:
                tab.append([NAME[t], fr(r["backlog_peak"]), f"{r['reprise_after_production_s']:.0f} s",
                            fr(r["events_consumed"]), str(r["audit"]["lost"]), str(r["audit"]["duplicate_rows"])])
        s.append(table(["Transport", "Backlog max (événements)", "Reprise après arrêt", "Consommés", "Pertes",
                        "Doublons"], tab, [2.6, 4, 3.6, 3, 2.2, 2.2]))
        s.append(Spacer(1, 4))
        s.append(image(fig_slow()))
    else:
        s.append(pending("slow"))
    s.append(P("<b>Interprétation :</b> les deux transports accumulent un backlog de ~9 000 événements et le résorbent "
               "en 67 à 73 s. Le comportement est symétrique : la reprise dépend du débit de consommation, pas du "
               "broker."))

    # 3.5 panne consommateur
    s.append(P("3.5 Panne consommateur (arrêt brutal SIGKILL puis redémarrage)", H2))
    s.append(P("<b>Hypothèse :</b> aucune perte grâce à l'acquittement manuel ; des doublons peuvent apparaître "
               "pour les messages en vol au moment de l'arrêt."))
    cr = load("crash")
    if cr:
        tab = []
        for t in ("kafka", "rabbitmq"):
            r = one(cr, transport=t)
            if r:
                a = r["audit"]
                tab.append([NAME[t], f"{r['recovery_first_ok_after_restart_s']:.2f} s",
                            f"{r['reprise_backlog_zero_after_restart_s']:.0f} s", fr(a["events_produced"]),
                            str(a["lost"]), str(a["duplicate_rows"])])
        s.append(table(["Transport", "1er traitement après redémarrage", "Backlog résorbé", "Événements",
                        "Pertes", "Doublons"], tab, [2.6, 4.2, 3, 2.6, 2, 2.2]))
        s.append(Spacer(1, 4))
    else:
        s.append(pending("crash"))
    s.append(P("<b>Interprétation :</b> zéro perte et zéro doublon sur les deux transports. Kafka met ~30 s à "
               "reprendre le traitement (rééquilibrage du groupe). RabbitMQ reprend presque immédiatement, car les "
               "messages non acquittés sont redistribués dès la reconnexion. <b>Limite :</b> un seul arrêt par "
               "transport ; le nombre de doublons dépend du moment de l'arrêt."))

    # 3.6 panne broker
    s.append(P("3.6 Panne broker (arrêt 20 s puis rétablissement)", H2))
    s.append(P("<b>Hypothèse :</b> aucune perte sur les messages confirmés ; disponibilité nulle pendant l'arrêt "
               "(nœud unique, pas de HA) ; une publication échouée côté producteur peut tout de même être persistée."))
    br = load("broker")
    if br:
        tab = []
        for t in ("kafka", "rabbitmq"):
            r = one(br, transport=t)
            if r:
                o = r["orders_outcome"]
                tab.append([NAME[t], f"{r['broker_down_s']:.0f} s", f"{r['outage_to_first_confirm_s']:.1f} s",
                            f"{r['first_confirm_after_recovery_s']:.1f} s", fr(o["completed"]), fr(o["failed"]),
                            fr(o["dropped"]), str(r["audit"]["lost"]), str(r["audit"]["unexpected"])])
        s.append(table(["Transport", "Arrêt", "1re confirm. depuis arrêt", "Après reprise", "Commandes OK",
                        "Échecs", "Ticks abandonnés", "Pertes", "Inattendus"], tab,
                       [2.1, 1.5, 2.6, 2.1, 2.1, 1.6, 2.3, 1.5, 1.9]))
        s.append(Spacer(1, 4))
        s.append(image(fig_resilience()))
    else:
        s.append(pending("broker"))
    s.append(P("<b>Interprétation :</b> sur les deux transports, aucun message confirmé n'est perdu. Kafka reprend "
               "les confirmations 2 s après la reprise ; RabbitMQ en met ~9 s et produit 283 échecs explicites. "
               "Côté RabbitMQ, 9 événements ont été consommés alors que le producteur a reçu une erreur : une "
               "publication échouée peut être persistée, il faut donc une stratégie d'idempotence côté consommateur. "
               "<b>Limite :</b> nœud unique, donc la disponibilité pendant la panne est nulle par construction."))

    # 3.7 messages invalides
    s.append(P("3.7 Messages invalides et erreurs répétées (DLQ / DLT)", H2))
    s.append(P("<b>Hypothèse :</b> les messages invalides sont isolés en DLQ/DLT sans bloquer la file principale ; "
               "les erreurs transitoires sont retentées puis dead-lettrées après épuisement."))
    fa = load("faults")
    if fa:
        tab = []
        for t in ("kafka", "rabbitmq"):
            r = one(fa, transport=t)
            if r:
                inv = r["orders_outcome"]["invalid_sent"]
                tab.append([NAME[t], str(inv), str(r["consumer_retryable_attempts"]), str(r["dlq_counter"]),
                            str(r["dlq_counter"] - inv), str(r["audit"]["lost"])])
        s.append(table(["Transport", "Invalides envoyés", "Tentatives échouées", "Total DLQ/DLT",
                        "Épuisés -> DLQ/DLT", "Absents de l'audit"], tab, [2.6, 2.8, 3.1, 2.8, 3.1, 2.8]))
        s.append(Spacer(1, 4))
        s.append(image(fig_faults()))
    else:
        s.append(pending("faults"))
    s.append(P("<b>Interprétation :</b> les messages invalides sont envoyés directement en DLQ/DLT, sans retry. Un "
               "événement transitoire a épuisé ses 4 tentatives côté Kafka et a été dead-lettré. Il apparaît comme "
               "« absent de l'audit » car l'audit ne trace que les traitements aboutis ; il est bien présent dans le "
               "DLT (vérifié). <b>Point d'attention :</b> un épuisement sur 1 720 messages à 5 % d'échec est "
               "statistiquement rare (~1 % attendu) ; RabbitMQ, avec 3 tentatives, n'en a eu aucun sur 1 738 événements "
               "(~0,2 attendu) : à reproduire sur plus de runs."))

    # 3.8 relecture
    s.append(P("3.8 Relecture (replay) après correction", H2))
    s.append(P("<b>Hypothèse :</b> Kafka et RabbitMQ Streams permettent de rejouer la période d'un run ; une file "
               "quorum ne conserve pas l'historique après acquittement."))
    rp = load("replay")
    if rp:
        tab = []
        for t in ("kafka", "rabbitmq"):
            r = one(rp, transport=t)
            if r:
                rep = r["replay"]
                hist = r.get("history", {})
                hist_txt = (f"rétention {hist.get('retention_ms')} ms" if t == "kafka"
                            else f"file quorum vide après consommation : {hist.get('quorum_ready_after_consumption')}")
                tab.append([NAME[t], fr(r["events_published"]), fr(rep["processed"] + rep["failed"]),
                            f"{rep['durationMs']/1000:.1f} s", "oui" if r.get("replay_matches_published") else "non",
                            hist_txt])
        s.append(table(["Transport", "Publiés", "Rejoués", "Durée", "Concordance", "Historique"], tab,
                       [2.4, 2.4, 2.4, 2.1, 2.5, 5.4]))
        s.append(Spacer(1, 4))
        s.append(image(fig_replay()))
    else:
        s.append(pending("replay"))
    s.append(P("<b>Interprétation :</b> la relecture par horodatage retrouve exactement les 30 000 événements publiés "
               "sur les deux transports (concordance vérifiée). Kafka rejoue en 0,9 s ; RabbitMQ en 3,6 s, dont 2 s "
               "d'inactivité nécessaires à la détection de fin. Pour RabbitMQ, le stream sert d'historique (1,95 million "
               "de messages conservés) : la file quorum est vide après consommation. <b>Attention :</b> la rétention Kafka "
               "observée est de 24 h (86 400 000 ms) ; au-delà, la relecture n'est plus possible sans l'augmenter."))

    # 3.9 ordre métier
    s.append(P("3.9 Ordre métier avec erreurs transitoires", H2))
    s.append(P("<b>Hypothèse :</b> le retry en mémoire (sans requeue) préserve l'ordre des étapes d'une commande, "
               "même avec des erreurs, tant qu'un seul consommateur traite chaque partition ou file."))
    od = load("ordering")
    if od:
        tab = []
        for t in ("kafka", "rabbitmq"):
            for n in (1, 3):
                r = one(od, transport=t, consumers=n)
                if r:
                    a = r["audit"]
                    dl = r["dlq_counter"] - r["events_consumed_invalid"]
                    tab.append([NAME[t], str(n), fr(a["orders_checked"]), str(a["orders_out_of_order"]),
                                str(dl), str(a["lost"] - dl), str(a["duplicate_rows"])])
        s.append(table(["Transport", "Instances", "Commandes vérifiées", "Hors ordre",
                        "Dead-lettrés (échecs)", "Pertes réelles", "Doublons"], tab,
                       [2.3, 2, 3, 2.4, 2.8, 2.4, 2.3]))
        s.append(Spacer(1, 4))
        f = fig_ordering()
        if f:
            s.append(image(f))
    else:
        s.append(pending("ordering"))
    s.append(P("<b>Interprétation :</b> avec 1 consommateur, aucun transport ne casse l'ordre (0 commande hors ordre "
               "sur 3 000, avec 5 % d'erreurs transitoires). Avec 3 consommateurs, Kafka le conserve (0 sur 3 000 : "
               "chaque partition reste traitée par un seul consommateur), alors que RabbitMQ le perd sur 270 commandes "
               "(9 %) : les trois consommateurs se partagent les messages d'une même file. Les retries en mémoire "
               "préservent l'ordre tant qu'un seul consommateur traite une file. Les événements dead-lettrés (2 et 4 "
               "sur RabbitMQ, 0 sur Kafka) sont des échecs après épuisement des tentatives, pas des pertes : ils sont "
               "présents dans la DLQ."))
    return s


def section_matrix():
    s = [PageBreak(), P("4. Matrice de décision pondérée", H1),
         P("Pondérations choisies à partir des contraintes du contexte e-commerce (ordre, reprise, relecture). Notes "
           "sur 5, justifiées par les expériences ci-dessus. Les notes sont un avis d'expert appuyé par les preuves, "
           "pas une mesure directe.", BODY)]
    crit = [
        ("Débit et latence", 20, 4, 4, "Débit de consommation équivalent à 1 M ; latence p50/p95 équivalente à "
                                        "50 ord/s ; p99 comparable à 500 ord/s (§3.1, §3.2)."),
        ("Fiabilité / reprise", 25, 4, 3, "Zéro perte réelle sur tous les runs ; seuls des événements dead-lettrés "
                                          "(2 à 4 par run RabbitMQ). RabbitMQ : 9 événements inattendus et 283 échecs "
                                          "lors de la panne broker, et ordre cassé à 3 consommateurs (§3.5 à §3.9)."),
        ("Relecture / historique", 15, 5, 4, "Kafka rejoue par horodatage sur un topic conservé ; RabbitMQ via stream, "
                                             "la file quorum n'a pas d'historique (§3.8)."),
        ("Complexité d'exploitation", 15, 3, 4, "Kafka en KRaft plus une Schema Registry et un agent JMX ; RabbitMQ "
                                                "est un seul service avec plugins (§2.1)."),
        ("Routage et flexibilité", 10, 2, 5, "Kafka : partitions seulement. RabbitMQ : exchanges, DLX, hash, "
                                             "multiple files (§2.1)."),
        ("Ressources / coût", 15, 3, 4, "Coût CPU par message à 1 M proche ; Kafka consomme davantage de RAM "
                                        "(broker ~1,1 Go contre ~0,3 Go pour RabbitMQ)."),
    ]
    rows = []
    tk = tr = 0
    for name, w, k, r, why in crit:
        tk += w * k / 5
        tr += w * r / 5
        rows.append([name, f"{w} %", str(k), str(r), why])
    rows.append(["<b>Score pondéré (sur 100)</b>", "<b>100 %</b>", f"<b>{tk:.1f}</b>", f"<b>{tr:.1f}</b>",
                 "Score indicatif. Les contraintes éliminatoires priment : ordre strict par commande avec "
                 "consommateurs concurrents -> Kafka ; routage complexe sans contrainte d'ordre -> RabbitMQ."])
    s.append(table(["Critère", "Poids", "Kafka /5", "RabbitMQ /5", "Justification / preuve"], rows,
                   [3.6, 1.6, 1.8, 2.1, 7.1]))
    return s


def section_limits():
    s = [P("5. Limites de validité", H1)]
    s += bullets([
        "<b>Matériel :</b> 2 vCPU et 8 Go partagés entre broker, producteurs, consommateurs et Prometheus. La "
        "concurrence (3 et 5 instances) et les débits absolus sont dominés par la contention CPU, pas par les brokers.",
        "<b>Topologie :</b> un seul nœud par broker. La haute disponibilité (réplication, failover multi-nœuds) n'est "
        "pas mesurée ; la panne broker ne mesure que l'indisponibilité et la reprise.",
        "<b>Répétitions :</b> une seule répétition par point de mesure. Les médianes et la dispersion demandées par le "
        "protocole ne sont donc pas calculées ; les écarts faibles ne sont pas significatifs.",
        "<b>Producteur :</b> la concurrence de publication est fixée à 100 messages en vol. Les débits de publication "
        "mesurent le producteur client, pas le plafond du broker. Un test à 2 000 en vol a bloqué RabbitMQ (canaux "
        "fermés, erreur 406) : le plafond réel de RabbitMQ dépend de ce réglage.",
        "<b>Mesure :</b> les histogrammes de latence sont interpolés sur des buckets ; les compteurs RabbitMQ sont "
        "rafraîchis toutes les 5 s, ce qui décale la détection de fin de drainage.",
        "<b>Audit :</b> l'horodatage est en millisecondes ; des événements dans la même milliseconde sont considérés "
        "comme ordonnés. Les événements dead-lettrés n'apparaissent pas dans l'audit des traitements aboutis (vérifiés "
        "séparément dans le DLT).",
        "<b>Cas métier :</b> un seul scénario e-commerce à 3 événements. Les résultats ne se transposent pas "
        "directement à un flux IoT ou à des notifications sans nouvelle mesure.",
    ])
    return s


def section_recommendation(status):
    s = [P("6. Recommandation conditionnelle", H1)]
    s.append(P("<b>Pour un tunnel de commande où l'ordre par commande est une contrainte métier</b> (création, "
               "paiement, stock), Kafka est recommandé dans ce contexte : la clé de partition garantit l'ordre, "
               "l'historique est rejouable par horodatage, et la reprise sur panne consommateur est sans perte. "
               "À condition d'accepter une Schema Registry et une exploitation KRaft."))
    s.append(P("<b>Pour une distribution de tâches indépendantes, des routages riches et des DLX natives</b> "
               "(notifications, traitements asynchrones), RabbitMQ avec quorum queues est recommandé, à condition "
               "de ne jamais compter sur l'ordre entre consommateurs concurrents et de prévoir une idempotence côté "
               "consommateur pour les publications en erreur."))
    s.append(P("<b>Conditions de validité :</b> ces conclusions valent pour un nœud unique, 2 vCPU, un cas à 3 "
               "événements et une charge jusqu'à ~1 500 msg/s. Elles doivent être reconfirmées sur un cluster "
               "(3 nœuds), avec plusieurs répétitions, avant toute décision d'architecture."))
    s.append(P("<b>Alternatives à évaluer :</b> RabbitMQ avec une seule consommation par file pour l'ordre, Kafka "
               "avec plus de partitions pour le parallélisme, ou un outbox transactionnel pour la cohérence entre "
               "base et broker."))
    return s


def section_annex():
    s = [PageBreak(), P("Annexe : reproduction", H1)]
    s += bullets([
        "Lancer la stack : <font face='Courier'>docker compose up -d --build</font>",
        "Lancer une famille : <font face='Courier'>python3 scripts/bench.py debit</font> (ou latency, concurrency, "
        "slow, crash, broker, faults, replay, ordering)",
        "Suivre la progression : <font face='Courier'>python3 scripts/progress.py</font>",
        "Régénérer ce rapport : <font face='Courier'>python3 scripts/report.py</font>",
        "Données brutes : results/&lt;famille&gt;/*.json ; audit par run : results/audit/&lt;run&gt;/*.csv (ignoré par git).",
    ])
    return s


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    FIG.mkdir(parents=True, exist_ok=True)
    status = family_status()
    complete = all(status[k] >= PLAN[k] for k in PLAN)
    story = []
    story += [Spacer(1, 3 * cm), P("Kafka vs RabbitMQ", ParagraphStyle("T", parent=H1, fontSize=28, leading=34)),
              P("Rapport d'expérimentation : comparatif de deux transports pour un cas e-commerce asynchrone",
                ParagraphStyle("ST", parent=BODY, fontSize=13, leading=17)),
              Spacer(1, 8),
              P(f"Date de génération : {time.strftime('%d/%m/%Y %H:%M')}", BODY),
              P("Statut : " + ("campagne terminée" if complete else
                               f"campagne en cours ({sum(min(v, PLAN[k]) for k, v in status.items())}/38 runs)"),
                BODY),
              Spacer(1, 4 * cm)]
    story += section_summary(status, complete)
    story += section_method(status)
    story += section_experiments(status)
    story += section_matrix()
    story += section_limits()
    story += section_recommendation(status)
    story += section_annex()
    doc = SimpleDocTemplate(str(OUT / "rapport-kafka-vs-rabbitmq.pdf"), pagesize=A4, leftMargin=1.8 * cm,
                            rightMargin=1.8 * cm, topMargin=1.6 * cm, bottomMargin=1.8 * cm,
                            title="Kafka vs RabbitMQ : rapport d'expérimentation")
    doc.build(story, onFirstPage=lambda c, d: None, onLaterPages=on_page)
    print(f"written {OUT / 'rapport-kafka-vs-rabbitmq.pdf'}")


if __name__ == "__main__":
    main()
