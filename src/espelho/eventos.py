"""Do evento do Debezium (JSON sem schema) para as ações no destino."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Acao:
    tipo: str     # upsert, delete ou truncate
    valores: dict  # linha inteira no upsert; só a chave no delete


def planejar(evento: dict | None, chave: tuple[str, ...]) -> list[Acao]:
    """Ações para um evento. Tombstone (valor nulo) não gera ação, só avança o offset."""
    if evento is None:
        return []
    op = evento.get("op")
    antes, depois = evento.get("before"), evento.get("after")
    if op in ("c", "r"):  # insert ou linha do snapshot inicial
        return [Acao("upsert", _exigir(depois, op))]
    if op == "u":
        depois = _exigir(depois, op)
        acoes = []
        # com chave natural, o update pode trocar a chave: a linha antiga precisa sair
        if antes and _chave(antes, chave) != _chave(depois, chave):
            acoes.append(Acao("delete", _chave(antes, chave)))
        return acoes + [Acao("upsert", depois)]
    if op == "d":
        return [Acao("delete", _chave(_exigir(antes, op), chave))]
    if op == "t":
        return [Acao("truncate", {})]
    raise ValueError(f"operação desconhecida: {op!r}")


def _exigir(linha: dict | None, op: str) -> dict:
    if not linha:
        raise ValueError(f"evento {op!r} sem a imagem da linha")
    return linha


def _chave(linha: dict, chave: tuple[str, ...]) -> dict:
    faltando = [c for c in chave if c not in linha]
    if faltando:
        raise ValueError(f"evento sem a coluna de chave {', '.join(faltando)}")
    return {c: linha[c] for c in chave}
