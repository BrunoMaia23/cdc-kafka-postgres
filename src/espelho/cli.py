"""Espelho Postgres -> Postgres via Debezium e Kafka. Um subcomando por etapa."""
from __future__ import annotations

import argparse
import sys

import psycopg

from . import carga_completa, catalogo as cat, conector, consumidor, dados, destino, reconciliacao
from .config import Config, carregar


def _fmt(contagem: dict) -> str:
    return ", ".join(f"{k} {v}" for k, v in contagem.items()) or "nenhum"


def preparar_destino(cfg: Config, catalogo: cat.Catalogo) -> None:
    with psycopg.connect(cfg.origem, autocommit=True) as origem, \
            psycopg.connect(cfg.destino, autocommit=True) as alvo:
        destino.preparar(origem, alvo, catalogo, cfg.schema_destino)


def imprimir_consumo(r: dict, catalogo: cat.Catalogo) -> None:
    por_tabela = {t.nome: r["aplicados"][t.nome] for t in catalogo.cdc() if r["aplicados"][t.nome]}
    print(f"[cdc]          {sum(r['aplicados'].values())} eventos aplicados ({_fmt(por_tabela)}), "
          f"{r['erros']} com erro, {r['tombstones']} tombstones, lag {r['lag']}")


def imprimir_reconciliacao(comparacoes: list) -> bool:
    for c in comparacoes:
        print(f"   {c.tabela:<12} origem {c.linhas_origem:>4} | destino {c.linhas_destino:>4} | "
              f"{'igual' if c.igual else 'DIFERENTE'}")
    ok = all(c.igual for c in comparacoes)
    print(f"[reconciliação] {'origem e destino iguais nas' if ok else 'DIVERGÊNCIA em'} "
          f"{sum(c.igual for c in comparacoes) if ok else sum(not c.igual for c in comparacoes)} tabelas")
    return ok


def demo(cfg: Config, catalogo: cat.Catalogo) -> int:
    if conector.existe(cfg, cfg.conector):
        print("Esta stack já rodou a demo. Para começar do zero:\n"
              "  docker compose down -v && docker compose up -d --wait")
        return 2
    print(f"[origem]       tabelas criadas com dados fictícios: {_fmt(dados.preparar_origem(cfg))}")
    preparar_destino(cfg, catalogo)
    print(f"[destino]      schema {cfg.schema_destino} com {len(catalogo.tabelas)} tabelas e o controle em _cdc")
    conector.registrar(cfg, catalogo, cfg.conector, cfg.slot)
    conector.aguardar_topicos(cfg, sorted(catalogo.por_topico()))
    print(f"[conector]     {cfg.conector} rodando, snapshot inicial de {len(catalogo.cdc())} tabelas")
    print(f"[carga]        {_fmt(carga_completa.carregar(cfg, catalogo))} linhas por COPY")
    imprimir_consumo(consumidor.consumir(cfg, catalogo), catalogo)

    print(f"[origem]       alterações: {_fmt(dados.alterar_origem(cfg))}")
    imprimir_consumo(consumidor.consumir(cfg, catalogo), catalogo)
    print(f"[carga]        {_fmt(carga_completa.carregar(cfg, catalogo))} linhas por COPY")
    iguais = imprimir_reconciliacao(reconciliacao.reconciliar(cfg, catalogo))

    de_novo = consumidor.consumir(cfg, catalogo, ocioso=3)
    reaplicados = sum(de_novo["aplicados"].values())
    print(f"[reinício]     consumidor rodou de novo e aplicou {reaplicados} eventos "
          "(o ponto de partida vem dos offsets gravados no destino)")
    return 0 if iguais and reaplicados == 0 else 1


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(prog="espelho", description=__doc__)
    sub = parser.add_subparsers(dest="comando", required=True)
    for nome, ajuda in [("demo", "roda tudo numa stack nova e confere origem x destino"),
                        ("preparar-origem", "cria a loja fictícia na origem"),
                        ("preparar-destino", "cria as tabelas espelho e o controle no destino"),
                        ("registrar-conector", "cria ou atualiza o conector do Debezium"),
                        ("carga-completa", "copia as tabelas sem chave por COPY"),
                        ("consumir", "aplica os eventos pendentes e para quando o lag zera"),
                        ("alterar-origem", "gera uma rajada de alterações na origem"),
                        ("reconciliar", "compara origem e destino tabela por tabela")]:
        sub.add_parser(nome, help=ajuda)
    args = parser.parse_args(argv)
    cfg, catalogo = carregar(), cat.carregar()

    if args.comando == "demo":
        return demo(cfg, catalogo)
    if args.comando == "preparar-origem":
        print(_fmt(dados.preparar_origem(cfg)))
    elif args.comando == "preparar-destino":
        preparar_destino(cfg, catalogo)
    elif args.comando == "registrar-conector":
        conector.registrar(cfg, catalogo, cfg.conector, cfg.slot)
    elif args.comando == "carga-completa":
        print(_fmt(carga_completa.carregar(cfg, catalogo)))
    elif args.comando == "consumir":
        imprimir_consumo(consumidor.consumir(cfg, catalogo), catalogo)
    elif args.comando == "alterar-origem":
        print(_fmt(dados.alterar_origem(cfg)))
    elif args.comando == "reconciliar":
        return 0 if imprimir_reconciliacao(reconciliacao.reconciliar(cfg, catalogo)) else 1
    return 0
