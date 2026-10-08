"""Banco de destino: tabelas espelho, controle de offsets e aplicação dos eventos.

O lote inteiro é uma transação, e o offset de cada partição é gravado nela. Se o processo cair no
meio, nada do lote fica no banco e a próxima execução relê a partir do último offset confirmado.
Cada evento roda num savepoint: um evento ruim vai para _cdc.erros e o resto do lote segue.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field

import psycopg

from . import eventos, sql, tipos
from .catalogo import Catalogo, Tabela

CONTROLE = [
    "CREATE SCHEMA IF NOT EXISTS _cdc",
    """CREATE TABLE IF NOT EXISTS _cdc.offsets (
        topico text NOT NULL, particao integer NOT NULL, proximo bigint NOT NULL,
        atualizado_em timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (topico, particao))""",
    """CREATE TABLE IF NOT EXISTS _cdc.erros (
        topico text NOT NULL, particao integer NOT NULL, "offset" bigint NOT NULL,
        chave text, valor text, erro text NOT NULL, registrado_em timestamptz NOT NULL DEFAULT now())""",
]


@dataclass(frozen=True)
class Mensagem:
    topico: str
    particao: int
    offset: int
    chave: bytes | None
    valor: bytes | None


@dataclass
class Resultado:
    aplicados: Counter = field(default_factory=Counter)  # eventos por tabela
    erros: int = 0
    tombstones: int = 0


def colunas_origem(con: psycopg.Connection, nome: str, schema: str = "public") -> list[tuple[str, str]]:
    """(coluna, tipo completo) na ordem da tabela, ex.: ('preco', 'numeric(12,2)')."""
    return con.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum""", [schema, nome]).fetchall()


def preparar(origem: psycopg.Connection, destino: psycopg.Connection, catalogo: Catalogo, schema: str) -> None:
    """Cria no destino as tabelas espelho, copiando os tipos da origem, e as tabelas de controle."""
    with destino.transaction():
        for comando in CONTROLE:
            destino.execute(comando)
        destino.execute(f"CREATE SCHEMA IF NOT EXISTS {sql.ident(schema)}")
        for t in catalogo.tabelas:
            colunas = colunas_origem(origem, t.nome)
            if not colunas:
                raise ValueError(f"{t.nome} não existe na origem")
            definicoes = [f"{sql.ident(nome)} {tipo}" for nome, tipo in colunas]
            if t.chave:  # no destino a chave natural vira PK: é ela que o upsert usa
                definicoes.append(f"PRIMARY KEY ({', '.join(sql.ident(c) for c in t.chave)})")
            destino.execute(f"CREATE TABLE IF NOT EXISTS {sql.tabela(schema, t.nome)} ({', '.join(definicoes)})")


def colunas_destino(con: psycopg.Connection, schema: str, catalogo: Catalogo) -> dict[str, dict]:
    """{tabela: {coluna: (tipo, precisão)}}, usado para converter os valores dos eventos."""
    resultado: dict[str, dict] = {}
    for t in catalogo.cdc():
        linhas = con.execute("""
            SELECT column_name, data_type, datetime_precision FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""", [schema, t.nome]).fetchall()
        resultado[t.nome] = {nome: (tipo, precisao) for nome, tipo, precisao in linhas}
    return resultado


def offsets(con: psycopg.Connection) -> dict[tuple[str, int], int]:
    return {(topico, particao): proximo for topico, particao, proximo in
            con.execute("SELECT topico, particao, proximo FROM _cdc.offsets").fetchall()}


def aplicar_lote(con: psycopg.Connection, schema: str, catalogo: Catalogo, colunas: dict[str, dict],
                 mensagens: list[Mensagem], falhar_no: int | None = None) -> Resultado:
    """Aplica o lote e grava os offsets na mesma transação. `falhar_no` simula uma queda (testes)."""
    por_topico = catalogo.por_topico()
    resultado = Resultado()
    proximos: dict[tuple[str, int], int] = {}
    with con.transaction():
        for i, m in enumerate(mensagens):
            if falhar_no is not None and i == falhar_no:
                raise RuntimeError("queda simulada no meio do lote")
            proximos[(m.topico, m.particao)] = m.offset + 1
            try:
                with con.transaction():
                    tabela = por_topico.get(m.topico)
                    if tabela is None:
                        raise ValueError(f"tópico fora do catálogo: {m.topico}")
                    evento = json.loads(m.valor) if m.valor is not None else None
                    acoes = eventos.planejar(evento, tabela.chave)
                    for acao in acoes:
                        _executar(con, schema, tabela, colunas[tabela.nome], acao)
                if acoes:
                    resultado.aplicados[tabela.nome] += 1
                else:
                    resultado.tombstones += 1
            except (ValueError, KeyError, TypeError, psycopg.Error) as exc:
                con.execute("""INSERT INTO _cdc.erros (topico, particao, "offset", chave, valor, erro)
                               VALUES (%s, %s, %s, %s, %s, %s)""",
                            [m.topico, m.particao, m.offset, _texto(m.chave), _texto(m.valor), str(exc)[:1000]])
                resultado.erros += 1
        for (topico, particao), proximo in proximos.items():
            con.execute("""INSERT INTO _cdc.offsets (topico, particao, proximo) VALUES (%s, %s, %s)
                           ON CONFLICT (topico, particao) DO UPDATE
                           SET proximo = EXCLUDED.proximo, atualizado_em = now()""", [topico, particao, proximo])
    return resultado


def _executar(con: psycopg.Connection, schema: str, tabela: Tabela, colunas: dict, acao: eventos.Acao) -> None:
    if acao.tipo == "upsert":
        linha = tipos.converter_linha(acao.valores, colunas)
        nomes = list(linha)
        con.execute(sql.upsert(schema, tabela.nome, nomes, tabela.chave), [linha[n] for n in nomes])
    elif acao.tipo == "delete":
        chave = tipos.converter_linha(acao.valores, colunas)
        con.execute(sql.delete(schema, tabela.nome, tabela.chave), [chave[c] for c in tabela.chave])
    elif acao.tipo == "truncate":
        con.execute(sql.truncate(schema, tabela.nome))


def _texto(dados: bytes | None) -> str | None:
    return dados.decode("utf-8", errors="replace") if dados is not None else None
