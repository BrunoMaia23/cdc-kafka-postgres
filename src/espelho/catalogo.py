"""Catálogo das tabelas espelhadas: a única lista de tabelas do projeto."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from pathlib import Path

ESTRATEGIAS = ("cdc", "carga_completa")
NOME_VALIDO = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class Tabela:
    nome: str
    estrategia: str
    chave: tuple[str, ...]
    chave_natural: bool = False  # sem PK na origem; a chave vem de um índice único


@dataclass(frozen=True)
class Catalogo:
    topico_prefixo: str
    tabelas: tuple[Tabela, ...]

    def cdc(self) -> list[Tabela]:
        return [t for t in self.tabelas if t.estrategia == "cdc"]

    def carga_completa(self) -> list[Tabela]:
        return [t for t in self.tabelas if t.estrategia == "carga_completa"]

    def topico(self, tabela: Tabela) -> str:
        return f"{self.topico_prefixo}.public.{tabela.nome}"

    def por_topico(self) -> dict[str, Tabela]:
        return {self.topico(t): t for t in self.cdc()}

    def com_prefixo(self, prefixo: str) -> Catalogo:
        return replace(self, topico_prefixo=prefixo)


def carregar(caminho: Path | str | None = None) -> Catalogo:
    arquivo = Path(caminho) if caminho else Path(__file__).with_name("catalogo.json")
    dados = json.loads(arquivo.read_text(encoding="utf-8"))
    tabelas = tuple(Tabela(t["nome"], t["estrategia"], tuple(t.get("chave", [])), t.get("chave_natural", False))
                    for t in dados["tabelas"])
    validar(tabelas)
    return Catalogo(dados["topico_prefixo"], tabelas)


def validar(tabelas: tuple[Tabela, ...]) -> None:
    nomes = [t.nome for t in tabelas]
    repetidos = sorted({n for n in nomes if nomes.count(n) > 1})
    if repetidos:
        raise ValueError(f"tabela repetida no catálogo: {', '.join(repetidos)}")
    for t in tabelas:
        if t.estrategia not in ESTRATEGIAS:
            raise ValueError(f"{t.nome}: estratégia desconhecida {t.estrategia!r}")
        if t.estrategia == "cdc" and not t.chave:
            raise ValueError(f"{t.nome}: CDC precisa de chave para aplicar update e delete")
        for nome in (t.nome, *t.chave):
            if not NOME_VALIDO.match(nome):
                raise ValueError(f"nome inválido: {nome!r}")
