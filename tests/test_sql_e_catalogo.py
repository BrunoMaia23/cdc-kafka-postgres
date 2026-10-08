import json

import pytest

from espelho import catalogo as cat, conector, sql


def test_upsert_com_chave_composta():
    assert sql.upsert("espelho", "item_pedido", ["pedido_id", "linha", "quantidade"], ("pedido_id", "linha")) == (
        'INSERT INTO "espelho"."item_pedido" ("pedido_id", "linha", "quantidade") VALUES (%s, %s, %s) '
        'ON CONFLICT ("pedido_id", "linha") DO UPDATE SET "quantidade" = EXCLUDED."quantidade"')


def test_upsert_so_com_a_chave_nao_atualiza_nada():
    assert sql.upsert("espelho", "t", ["id"], ("id",)).endswith('ON CONFLICT ("id") DO NOTHING')


def test_delete():
    assert sql.delete("espelho", "item_pedido", ("pedido_id", "linha")) == \
        'DELETE FROM "espelho"."item_pedido" WHERE "pedido_id" = %s AND "linha" = %s'


@pytest.mark.parametrize("nome", ['x"; DROP TABLE y; --', "Maiuscula", "1coluna", ""])
def test_nome_invalido_nunca_vira_sql(nome):
    with pytest.raises(ValueError):
        sql.ident(nome)


def test_catalogo_do_pacote():
    c = cat.carregar()
    assert [t.nome for t in c.cdc()] == ["cliente", "produto", "pedido", "item_pedido"]
    assert [t.nome for t in c.carga_completa()] == ["evento_site"]
    assert c.por_topico()["loja.public.item_pedido"].chave == ("pedido_id", "linha")
    assert c.com_prefixo("teste").topico(c.cdc()[0]) == "teste.public.cliente"


@pytest.mark.parametrize("tabelas, erro", [
    ([{"nome": "a", "estrategia": "cdc", "chave": []}], "precisa de chave"),
    ([{"nome": "a", "estrategia": "cdc", "chave": ["id"]}, {"nome": "a", "estrategia": "cdc", "chave": ["id"]}], "repetida"),
    ([{"nome": "a", "estrategia": "dms", "chave": ["id"]}], "estratégia"),
    ([{"nome": "a-b", "estrategia": "carga_completa"}], "inválido"),
])
def test_catalogo_invalido(tmp_path, tabelas, erro):
    arquivo = tmp_path / "catalogo.json"
    arquivo.write_text(json.dumps({"topico_prefixo": "x", "tabelas": tabelas}), encoding="utf-8")
    with pytest.raises(ValueError, match=erro):
        cat.carregar(arquivo)


def test_configuracao_do_conector_sai_do_catalogo():
    config = conector.configuracao(cat.carregar(), slot="espelho")
    assert config["table.include.list"] == "public.cliente,public.produto,public.pedido,public.item_pedido"
    assert config["message.key.columns"] == "public.produto:sku"
    assert config["topic.prefix"] == "loja"
    assert config["publication.name"] == "espelho_pub"
    assert config["value.converter.schemas.enable"] == "false"
