"""SQL do destino. Nomes validados e entre aspas; valores sempre por parâmetro."""
from __future__ import annotations

from .catalogo import NOME_VALIDO


def ident(nome: str) -> str:
    if not NOME_VALIDO.match(nome):
        raise ValueError(f"nome inválido: {nome!r}")
    return f'"{nome}"'


def tabela(schema: str, nome: str) -> str:
    return f"{ident(schema)}.{ident(nome)}"


def upsert(schema: str, nome: str, colunas: list[str], chave: tuple[str, ...]) -> str:
    lista = ", ".join(ident(c) for c in colunas)
    marcadores = ", ".join("%s" for _ in colunas)
    resto = [c for c in colunas if c not in chave]
    acao = ("DO UPDATE SET " + ", ".join(f"{ident(c)} = EXCLUDED.{ident(c)}" for c in resto)
            if resto else "DO NOTHING")
    alvo = ", ".join(ident(c) for c in chave)
    return f"INSERT INTO {tabela(schema, nome)} ({lista}) VALUES ({marcadores}) ON CONFLICT ({alvo}) {acao}"


def delete(schema: str, nome: str, chave: tuple[str, ...]) -> str:
    condicao = " AND ".join(f"{ident(c)} = %s" for c in chave)
    return f"DELETE FROM {tabela(schema, nome)} WHERE {condicao}"


def truncate(schema: str, nome: str) -> str:
    return f"TRUNCATE {tabela(schema, nome)}"
