import pytest

from espelho.eventos import Acao, planejar

CHAVE = ("id",)


def test_insert_e_snapshot_viram_upsert():
    for op in ("c", "r"):
        assert planejar({"op": op, "after": {"id": 1, "nome": "Ana"}}, CHAVE) == [Acao("upsert", {"id": 1, "nome": "Ana"})]


def test_update_sem_troca_de_chave():
    evento = {"op": "u", "before": {"id": 1, "nome": "Ana"}, "after": {"id": 1, "nome": "Ana Lopes"}}
    assert planejar(evento, CHAVE) == [Acao("upsert", {"id": 1, "nome": "Ana Lopes"})]


def test_update_que_troca_a_chave_apaga_a_linha_antiga():
    evento = {"op": "u", "before": {"sku": "SKU-0003", "preco": "10.00"}, "after": {"sku": "SKU-9001", "preco": "10.00"}}
    assert planejar(evento, ("sku",)) == [Acao("delete", {"sku": "SKU-0003"}),
                                          Acao("upsert", {"sku": "SKU-9001", "preco": "10.00"})]


def test_update_sem_imagem_anterior_so_faz_upsert():
    assert planejar({"op": "u", "before": None, "after": {"id": 2}}, CHAVE) == [Acao("upsert", {"id": 2})]


def test_delete_usa_so_a_chave():
    evento = {"op": "d", "before": {"pedido_id": 7, "linha": 2, "sku": "SKU-0001"}, "after": None}
    assert planejar(evento, ("pedido_id", "linha")) == [Acao("delete", {"pedido_id": 7, "linha": 2})]


def test_tombstone_nao_gera_acao():
    assert planejar(None, CHAVE) == []


def test_truncate():
    assert planejar({"op": "t"}, CHAVE) == [Acao("truncate", {})]


@pytest.mark.parametrize("evento", [
    {"op": "c", "after": None},
    {"op": "d", "before": None},
    {"op": "d", "before": {"nome": "sem id"}},
    {"op": "x", "after": {"id": 1}},
])
def test_evento_invalido(evento):
    with pytest.raises(ValueError):
        planejar(evento, CHAVE)
