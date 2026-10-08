"""Endereços da stack. O padrão aponta para o docker-compose deste repositório."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    origem: str = "postgresql://loja@localhost:55432/loja"
    destino: str = "postgresql://espelho@localhost:55433/espelho"
    kafka: str = "localhost:59092"
    connect: str = "http://localhost:58083"
    schema_destino: str = "espelho"
    conector: str = "loja-origem"
    slot: str = "espelho"


def carregar() -> Config:
    padrao = Config()
    return Config(
        origem=os.environ.get("ESPELHO_ORIGEM", padrao.origem),
        destino=os.environ.get("ESPELHO_DESTINO", padrao.destino),
        kafka=os.environ.get("ESPELHO_KAFKA", padrao.kafka),
        connect=os.environ.get("ESPELHO_CONNECT", padrao.connect),
    )
