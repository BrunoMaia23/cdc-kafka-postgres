"""Valores do Debezium (JSON sem schema) para tipos Python, pelo tipo da coluna no destino.

Sem schema na mensagem, quem diz como ler um número é a coluna de destino. Com o conector no modo
adaptive (padrão), timestamp com até 3 casas chega em milissegundos e acima disso em microssegundos;
date chega em dias desde 1970; numeric chega como texto por causa do decimal.handling.mode=string.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

EPOCA = datetime(1970, 1, 1)
INTEIROS = ("smallint", "integer", "bigint")


def converter(valor, tipo: str, precisao: int | None = None):
    if valor is None:
        return None
    if tipo == "timestamp without time zone":
        micros = int(valor) * 1_000 if precisao is not None and precisao <= 3 else int(valor)
        return EPOCA + timedelta(microseconds=micros)
    if tipo == "timestamp with time zone":
        return datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    if tipo == "date":
        return date(1970, 1, 1) + timedelta(days=int(valor))
    if tipo == "numeric":
        try:
            return Decimal(str(valor))
        except InvalidOperation:
            raise ValueError(f"numeric inválido: {valor!r}") from None
    if tipo in INTEIROS:
        if isinstance(valor, bool) or (isinstance(valor, float) and not valor.is_integer()):
            raise ValueError(f"{tipo} inválido: {valor!r}")
        return int(valor)
    return valor


def converter_linha(valores: dict, colunas: dict[str, tuple[str, int | None]]) -> dict:
    """Converte uma linha inteira. Coluna que o destino não conhece é erro, não descarte."""
    desconhecidas = sorted(set(valores) - set(colunas))
    if desconhecidas:
        raise ValueError(f"coluna que não existe no destino: {', '.join(desconhecidas)}")
    return {nome: converter(valor, *colunas[nome]) for nome, valor in valores.items()}
