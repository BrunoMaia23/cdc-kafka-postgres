"""Carga completa por COPY, para tabela sem chave confiável.

TRUNCATE e COPY na mesma transação: quem lê o destino vê a versão anterior inteira até o commit.
"""
from __future__ import annotations

import psycopg

from . import destino, sql
from .catalogo import Catalogo
from .config import Config


def carregar(cfg: Config, catalogo: Catalogo) -> dict[str, int]:
    linhas: dict[str, int] = {}
    with psycopg.connect(cfg.origem, autocommit=True) as origem, \
            psycopg.connect(cfg.destino, autocommit=True) as alvo:
        for t in catalogo.carga_completa():
            colunas = ", ".join(sql.ident(nome) for nome, _ in destino.colunas_origem(origem, t.nome))
            tabela_destino = sql.tabela(cfg.schema_destino, t.nome)
            with alvo.transaction():
                alvo.execute(f"TRUNCATE {tabela_destino}")
                with origem.cursor().copy(f"COPY (SELECT {colunas} FROM {sql.tabela('public', t.nome)}) TO STDOUT") as saida, \
                        alvo.cursor().copy(f"COPY {tabela_destino} ({colunas}) FROM STDIN") as entrada:
                    for bloco in saida:
                        entrada.write(bloco)
            linhas[t.nome] = alvo.execute(f"SELECT count(*) FROM {tabela_destino}").fetchone()[0]
    return linhas
