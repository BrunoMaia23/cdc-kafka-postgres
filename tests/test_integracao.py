"""Ponta a ponta na stack do docker-compose: pytest -m integracao (com a stack no ar).

Cada execução usa um prefixo próprio (conector, slot, tópicos e schema), então roda na mesma stack
da demo sem interferir nela, e limpa o que criou no final.
"""
import json
import time
import urllib.request
from dataclasses import replace

import psycopg
import pytest
from confluent_kafka import Producer

from espelho import carga_completa, catalogo as cat, conector, consumidor, dados, destino, reconciliacao
from espelho.config import carregar

pytestmark = pytest.mark.integracao


@pytest.fixture(scope="module")
def stack():
    base = carregar()
    try:
        urllib.request.urlopen(f"{base.connect}/connectors", timeout=5).close()
    except OSError:
        pytest.skip("stack fora do ar: docker compose up -d --wait")
    sufixo = f"t{int(time.time())}"
    cfg = replace(base, schema_destino=f"espelho_{sufixo}", conector=f"teste-{sufixo}", slot=f"espelho_{sufixo}")
    catalogo = cat.carregar().com_prefixo(sufixo)
    with psycopg.connect(cfg.origem, autocommit=True) as con:
        loja_existe = con.execute("SELECT to_regclass('public.cliente') IS NOT NULL").fetchone()[0]
    if not loja_existe:
        dados.preparar_origem(cfg)
    with psycopg.connect(cfg.origem, autocommit=True) as origem, psycopg.connect(cfg.destino, autocommit=True) as alvo:
        destino.preparar(origem, alvo, catalogo, cfg.schema_destino)
    conector.registrar(cfg, catalogo, cfg.conector, cfg.slot)
    conector.aguardar_topicos(cfg, sorted(catalogo.por_topico()))
    yield cfg, catalogo
    _limpar(cfg, catalogo)


def _limpar(cfg, catalogo):
    pedido = urllib.request.Request(f"{cfg.connect}/connectors/{cfg.conector}", method="DELETE")
    urllib.request.urlopen(pedido, timeout=30).close()
    with psycopg.connect(cfg.origem, autocommit=True) as con:
        for _ in range(30):  # o slot só pode sair depois que a task do conector parar
            ativo = con.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", [cfg.slot]).fetchone()
            if ativo is None:
                break
            if not ativo[0]:
                con.execute("SELECT pg_drop_replication_slot(%s)", [cfg.slot])
                break
            time.sleep(1)
        con.execute(f'DROP PUBLICATION IF EXISTS "{cfg.slot}_pub"')
    with psycopg.connect(cfg.destino, autocommit=True) as con:
        con.execute(f'DROP SCHEMA IF EXISTS "{cfg.schema_destino}" CASCADE')
        con.execute("DELETE FROM _cdc.offsets WHERE topico LIKE %s", [f"{catalogo.topico_prefixo}.%"])
        con.execute("DELETE FROM _cdc.erros WHERE topico LIKE %s", [f"{catalogo.topico_prefixo}.%"])


def _offsets_do_teste(cfg, catalogo):
    with psycopg.connect(cfg.destino, autocommit=True) as con:
        return {k: v for k, v in destino.offsets(con).items() if k[0].startswith(catalogo.topico_prefixo + ".")}


def _tudo_igual(cfg, catalogo):
    carga_completa.carregar(cfg, catalogo)
    return all(c.igual for c in reconciliacao.reconciliar(cfg, catalogo))


def test_espelho_ponta_a_ponta(stack):
    cfg, catalogo = stack

    # queda no meio do primeiro lote: o rollback leva os dados e o offset juntos
    with pytest.raises(RuntimeError, match="queda simulada"):
        consumidor.consumir(cfg, catalogo, falhar_no=5)
    assert _offsets_do_teste(cfg, catalogo) == {}

    r = consumidor.consumir(cfg, catalogo)
    assert r["erros"] == 0 and sum(r["aplicados"].values()) > 0
    assert _tudo_igual(cfg, catalogo)

    # evento malformado no meio de alterações normais: vai para _cdc.erros e o resto segue
    topico_cliente = catalogo.topico(catalogo.cdc()[0])
    produtor = Producer({"bootstrap.servers": cfg.kafka})
    produtor.produce(topico_cliente, key=b'{"id":"abc"}',
                     value=json.dumps({"op": "c", "after": {"id": "abc", "nome": "x"}}).encode())
    produtor.flush(10)
    marca = int(time.time() * 1000) % 100_000
    with psycopg.connect(cfg.origem, autocommit=True) as con, con.transaction():
        con.execute("INSERT INTO cliente (id, nome, criado_em) VALUES (%s, 'Teste', now())", [100_000 + marca])
        con.execute("UPDATE cliente SET cidade = 'Natal' WHERE id = %s", [100_000 + marca])
        con.execute("UPDATE produto SET preco = preco + 1 WHERE sku = (SELECT min(sku) FROM produto)")
        con.execute("DELETE FROM cliente WHERE id = %s", [100_000 + marca])

    r = consumidor.consumir(cfg, catalogo)
    assert r["erros"] == 1
    assert _tudo_igual(cfg, catalogo)
    with psycopg.connect(cfg.destino, autocommit=True) as con:
        erro = con.execute("SELECT erro FROM _cdc.erros WHERE topico = %s", [topico_cliente]).fetchone()[0]
    assert "integer" in erro or "invalid literal" in erro

    # reinício: o ponto de partida é o offset gravado no destino, nada é reaplicado
    r = consumidor.consumir(cfg, catalogo, ocioso=3)
    assert sum(r["aplicados"].values()) == 0 and r["erros"] == 0
