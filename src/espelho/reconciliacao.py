"""Origem x destino, tabela por tabela: contagem e hash das linhas na ordem da chave."""
from __future__ import annotations

from dataclasses import dataclass

import psycopg

from . import destino, sql
from .catalogo import Catalogo
from .config import Config


@dataclass(frozen=True)
class Comparacao:
    tabela: str
    linhas_origem: int
    linhas_destino: int
    igual: bool


def assinatura(con: psycopg.Connection, schema: str, nome: str, colunas: list[str],
               chave: tuple[str, ...]) -> tuple[int, str]:
    lista = ", ".join(sql.ident(c) for c in colunas)
    ordem = ", ".join(f"t.{sql.ident(c)}" for c in chave) if chave else "t::text"
    return con.execute(f"""
        SELECT count(*), md5(coalesce(string_agg(md5(t::text), '' ORDER BY {ordem}), ''))
        FROM (SELECT {lista} FROM {sql.tabela(schema, nome)}) t""").fetchone()


def reconciliar(cfg: Config, catalogo: Catalogo) -> list[Comparacao]:
    resultado = []
    with psycopg.connect(cfg.origem, autocommit=True) as origem, \
            psycopg.connect(cfg.destino, autocommit=True) as alvo:
        for t in catalogo.tabelas:
            colunas = [nome for nome, _ in destino.colunas_origem(origem, t.nome)]
            n_origem, hash_origem = assinatura(origem, "public", t.nome, colunas, t.chave)
            n_destino, hash_destino = assinatura(alvo, cfg.schema_destino, t.nome, colunas, t.chave)
            resultado.append(Comparacao(t.nome, n_origem, n_destino, hash_origem == hash_destino))
    return resultado
