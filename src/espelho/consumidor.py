"""Leitura dos tópicos do Debezium e aplicação no destino, em lotes."""
from __future__ import annotations

import time
from collections import Counter

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError

from . import destino
from .catalogo import Catalogo
from .config import Config

IGNORAVEIS = (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART)


def consumir(cfg: Config, catalogo: Catalogo, *, ocioso: float = 5.0, lote: int = 500,
             limite: float = 300.0, falhar_no: int | None = None) -> dict:
    """Consome até passar `ocioso` segundos sem mensagem e com lag zero."""
    total = {"aplicados": Counter(), "erros": 0, "tombstones": 0, "lotes": 0}
    with psycopg.connect(cfg.destino, autocommit=True) as con:
        colunas = destino.colunas_destino(con, cfg.schema_destino, catalogo)
        guardados = destino.offsets(con)

        def ao_atribuir(consumidor: Consumer, particoes: list) -> None:
            # o ponto de partida vem do destino, não do grupo de consumo do Kafka
            for p in particoes:
                p.offset = guardados.get((p.topic, p.partition), OFFSET_BEGINNING)
            consumidor.assign(particoes)

        consumidor = Consumer({
            "bootstrap.servers": cfg.kafka,
            "group.id": f"espelho-{catalogo.topico_prefixo}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        consumidor.subscribe(sorted(catalogo.por_topico()), on_assign=ao_atribuir)
        inicio = ultimo = time.monotonic()
        try:
            while True:
                if time.monotonic() - inicio > limite:
                    raise TimeoutError(f"consumo passou de {limite:.0f}s sem zerar o lag")
                mensagens = []
                for m in consumidor.consume(num_messages=lote, timeout=1.0):
                    if m.error():
                        if m.error().code() in IGNORAVEIS:
                            continue
                        raise RuntimeError(f"erro do Kafka: {m.error()}")
                    mensagens.append(destino.Mensagem(m.topic(), m.partition(), m.offset(), m.key(), m.value()))
                if mensagens:
                    r = destino.aplicar_lote(con, cfg.schema_destino, catalogo, colunas, mensagens, falhar_no)
                    total["aplicados"] += r.aplicados
                    total["erros"] += r.erros
                    total["tombstones"] += r.tombstones
                    total["lotes"] += 1
                    ultimo = time.monotonic()
                elif time.monotonic() - ultimo >= ocioso and lag(consumidor, con) == 0:
                    break
        finally:
            consumidor.close()
    total["lag"] = 0
    return total


def lag(consumidor: Consumer, con: psycopg.Connection) -> int | None:
    """Mensagens no Kafka ainda não confirmadas no destino. None enquanto não há partição atribuída."""
    atribuidas = consumidor.assignment()
    if not atribuidas:
        return None
    guardados = destino.offsets(con)
    pendente = 0
    for tp in atribuidas:
        baixo, alto = consumidor.get_watermark_offsets(tp, timeout=10)
        pendente += alto - guardados.get((tp.topic, tp.partition), baixo)
    return pendente
