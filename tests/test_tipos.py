from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from espelho.tipos import converter, converter_linha


def test_timestamp_em_microssegundos_e_milissegundos():
    assert converter(1_767_600_000_123_456, "timestamp without time zone", 6) == datetime(2026, 1, 5, 8, 0, 0, 123456)
    assert converter(1_767_600_000_123, "timestamp without time zone", 3) == datetime(2026, 1, 5, 8, 0, 0, 123000)


@pytest.mark.parametrize("texto, micros", [
    ("2026-01-05T08:00:00.5Z", 500000),
    ("2026-01-05T08:00:00.123456789Z", 123456),
    ("2026-01-05T08:00:00Z", 0),
    ("2026-01-05T05:00:00.25-03:00", 250000),
])
def test_timestamptz_com_z_e_fracao_de_qualquer_tamanho(texto, micros):
    assert converter(texto, "timestamp with time zone") == datetime(2026, 1, 5, 8, 0, 0, micros, tzinfo=timezone.utc)


def test_date_em_dias_desde_1970():
    assert converter(20458, "date") == date(2026, 1, 5)


def test_numeric_chega_como_texto():
    assert converter("12.30", "numeric") == Decimal("12.30")
    with pytest.raises(ValueError):
        converter("doze", "numeric")


def test_inteiro():
    assert converter(7, "integer") == 7
    for ruim in ("abc", True, 1.5):
        with pytest.raises((ValueError, TypeError)):
            converter(ruim, "integer")


def test_nulo_e_tipos_sem_conversao():
    assert converter(None, "date") is None
    assert converter("Recife", "text") == "Recife"
    assert converter(False, "boolean") is False


def test_linha_com_coluna_que_o_destino_nao_conhece():
    colunas = {"id": ("integer", None), "nome": ("text", None)}
    assert converter_linha({"id": 1, "nome": "Ana"}, colunas) == {"id": 1, "nome": "Ana"}
    with pytest.raises(ValueError, match="telefone"):
        converter_linha({"id": 1, "telefone": "0000"}, colunas)
