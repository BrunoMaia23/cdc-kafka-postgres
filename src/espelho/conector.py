"""Conector do Debezium: configuração gerada do catálogo e registro pela API REST do Kafka Connect."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from confluent_kafka.admin import AdminClient

from .catalogo import Catalogo
from .config import Config


def configuracao(catalogo: Catalogo, slot: str, host: str = "origem", porta: int = 5432,
                 usuario: str = "loja", banco: str = "loja") -> dict:
    tabelas = ",".join(f"public.{t.nome}" for t in catalogo.cdc())
    naturais = ";".join(f"public.{t.nome}:{','.join(t.chave)}" for t in catalogo.cdc() if t.chave_natural)
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": host,
        "database.port": str(porta),
        "database.user": usuario,
        "database.password": "demo",  # a origem da demo aceita conexão sem senha
        "database.dbname": banco,
        "topic.prefix": catalogo.topico_prefixo,
        "table.include.list": tabelas,
        "slot.name": slot,
        "publication.name": f"{slot}_pub",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    if naturais:  # sem PK na origem, a chave da mensagem precisa ser dita ao Debezium
        config["message.key.columns"] = naturais
    return config


def existe(cfg: Config, nome: str) -> bool:
    return nome in (_http("GET", f"{cfg.connect}/connectors") or [])


def registrar(cfg: Config, catalogo: Catalogo, nome: str, slot: str, espera: float = 120) -> None:
    """Cria ou atualiza o conector (PUT é idempotente) e espera ele e a task ficarem RUNNING."""
    _http("PUT", f"{cfg.connect}/connectors/{nome}/config", configuracao(catalogo, slot))
    limite = time.monotonic() + espera
    while time.monotonic() < limite:
        try:
            status = _http("GET", f"{cfg.connect}/connectors/{nome}/status")
        except urllib.error.HTTPError:
            status = None
        if status:
            estados = [status["connector"]["state"]] + [t["state"] for t in status.get("tasks", [])]
            if "FAILED" in estados:
                traco = next((t.get("trace", "") for t in status.get("tasks", []) if t["state"] == "FAILED"), "")
                raise RuntimeError(f"conector {nome} falhou: {traco[:500]}")
            if status.get("tasks") and set(estados) == {"RUNNING"}:
                return
        time.sleep(1)
    raise TimeoutError(f"conector {nome} não ficou RUNNING em {espera:.0f}s")


def aguardar_topicos(cfg: Config, topicos: list[str], espera: float = 120) -> None:
    """O Debezium cria o tópico de cada tabela no primeiro evento (o snapshot inicial)."""
    admin = AdminClient({"bootstrap.servers": cfg.kafka})
    limite = time.monotonic() + espera
    while time.monotonic() < limite:
        existentes = set(admin.list_topics(timeout=10).topics)
        faltando = sorted(set(topicos) - existentes)
        if not faltando:
            return
        time.sleep(1)
    raise TimeoutError(f"tópicos não apareceram: {', '.join(faltando)}")


def _http(metodo: str, url: str, corpo: dict | None = None):
    dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    pedido = urllib.request.Request(url, data=dados, method=metodo, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(pedido, timeout=30) as resposta:
        texto = resposta.read()
    return json.loads(texto) if texto else None
