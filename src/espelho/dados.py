"""Origem fictícia: uma loja com tabelas de formatos diferentes e uma rajada de alterações."""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

import psycopg

from .config import Config

TABELAS = """
CREATE TABLE cliente (
    id integer PRIMARY KEY, nome text NOT NULL, email text, cidade text, criado_em timestamp NOT NULL);
CREATE TABLE produto (
    sku text NOT NULL, nome text NOT NULL, preco numeric(12,2) NOT NULL, ativo boolean NOT NULL DEFAULT true);
CREATE UNIQUE INDEX produto_sku ON produto (sku);
ALTER TABLE produto REPLICA IDENTITY USING INDEX produto_sku;
CREATE TABLE pedido (
    id integer PRIMARY KEY, cliente_id integer NOT NULL REFERENCES cliente (id), data date NOT NULL,
    status text NOT NULL, atualizado_em timestamptz NOT NULL DEFAULT now());
CREATE TABLE item_pedido (
    pedido_id integer NOT NULL REFERENCES pedido (id) ON DELETE CASCADE, linha integer NOT NULL,
    sku text NOT NULL, quantidade integer NOT NULL, preco numeric(12,2) NOT NULL,
    PRIMARY KEY (pedido_id, linha));
CREATE TABLE evento_site (ocorrido_em timestamp NOT NULL, pagina text NOT NULL, visitante text);
"""
# produto não tem PK: a chave natural é o sku, e o REPLICA IDENTITY faz o update e o delete
# levarem o sku antigo para o log. evento_site não tem chave nenhuma e vai por carga completa.

NOMES = ["Ana", "Bia", "Caio", "Davi", "Elisa", "Fábio", "Gabi", "Hugo", "Íris", "Júlia",
         "Kauã", "Lara", "Miguel", "Nina", "Otávio", "Paula", "Renan", "Sara", "Tiago", "Vera"]
SOBRENOMES = ["Almeida", "Barros", "Campos", "Duarte", "Esteves", "Farias", "Gomes", "Lopes",
              "Moura", "Nunes", "Prado", "Rocha", "Sales", "Teixeira", "Vieira"]
CIDADES = ["Recife", "Curitiba", "Belém", "Natal", "Porto Alegre", "Salvador", "Goiânia",
           "Manaus", "Vitória", "Florianópolis"]
ITENS = ["Caneca", "Camiseta", "Caderno", "Mochila", "Garrafa", "Boné", "Agenda", "Chaveiro",
         "Adesivo", "Pôster"]
STATUS = ["novo", "pago", "enviado", "entregue", "cancelado"]
PAGINAS = ["/", "/produtos", "/carrinho", "/checkout", "/contato", "/sobre"]
INICIO = datetime(2026, 1, 5, 8, 0)


def preparar_origem(cfg: Config, semente: int = 42) -> dict[str, int]:
    """Cria as tabelas da loja e a carga inicial. Devolve as linhas por tabela."""
    rng = random.Random(semente)
    with psycopg.connect(cfg.origem, autocommit=True) as con, con.transaction():
        con.execute(TABELAS)
        for i in range(1, 41):
            _inserir_cliente(con, rng, i)
        for i in range(1, 26):
            item = ITENS[i % len(ITENS)]
            con.execute("INSERT INTO produto (sku, nome, preco) VALUES (%s, %s, %s)",
                        [f"SKU-{i:04d}", f"{item} modelo {i}", round(rng.uniform(9, 250), 2)])
        for i in range(1, 61):
            _inserir_pedido(con, rng, i, cliente=rng.randint(1, 40))
        for _ in range(300):
            _inserir_evento(con, rng)
        return _contagens(con)


def alterar_origem(cfg: Config, semente: int = 7) -> dict[str, int]:
    """Uma rajada de inserts, updates e deletes, com os casos que costumam quebrar um espelho."""
    rng = random.Random(semente)
    feito = {"insert": 0, "update": 0, "delete": 0}
    with psycopg.connect(cfg.origem, autocommit=True) as con, con.transaction():
        for i in rng.sample(range(1, 41), 8):
            con.execute("UPDATE cliente SET email = %s, cidade = %s WHERE id = %s",
                        [f"cliente{i}.novo@example.com", rng.choice(CIDADES), i])
            feito["update"] += 1
        for i in rng.sample(range(1, 26), 6):
            con.execute("UPDATE produto SET preco = round(preco * 1.1, 2) WHERE sku = %s", [f"SKU-{i:04d}"])
            feito["update"] += 1
        # troca da chave natural: no destino o sku antigo tem que sumir
        con.execute("UPDATE produto SET sku = 'SKU-9001' WHERE sku = 'SKU-0003'")
        feito["update"] += 1
        # mesma chave apagada e recriada dentro da mesma rajada
        _inserir_cliente(con, rng, 900)
        con.execute("DELETE FROM cliente WHERE id = 900")
        _inserir_cliente(con, rng, 900)
        feito["insert"] += 2
        feito["delete"] += 1
        for i in range(61, 73):
            _inserir_pedido(con, rng, i, cliente=rng.choice([900, *range(1, 41)]))
            feito["insert"] += 1
        for i in rng.sample(range(1, 61), 5):
            con.execute("UPDATE pedido SET status = %s, atualizado_em = now() WHERE id = %s",
                        [rng.choice(STATUS), i])
            feito["update"] += 1
        for i in rng.sample(range(1, 61), 4):  # os itens saem junto, pelo ON DELETE CASCADE
            con.execute("DELETE FROM pedido WHERE id = %s", [i])
            feito["delete"] += 1
        for _ in range(50):
            _inserir_evento(con, rng)
    return feito


def _inserir_cliente(con: psycopg.Connection, rng: random.Random, i: int) -> None:
    nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
    con.execute("INSERT INTO cliente (id, nome, email, cidade, criado_em) VALUES (%s, %s, %s, %s, %s)",
                [i, nome, f"cliente{i}@example.com", rng.choice(CIDADES),
                 INICIO + timedelta(minutes=rng.randint(0, 60 * 24 * 200), microseconds=rng.randint(0, 999_999))])


def _inserir_pedido(con: psycopg.Connection, rng: random.Random, i: int, cliente: int) -> None:
    con.execute("INSERT INTO pedido (id, cliente_id, data, status) VALUES (%s, %s, %s, %s)",
                [i, cliente, date(2026, 1, 5) + timedelta(days=rng.randint(0, 250)), rng.choice(STATUS)])
    for linha in range(1, rng.randint(1, 4) + 1):
        con.execute("INSERT INTO item_pedido (pedido_id, linha, sku, quantidade, preco) VALUES (%s, %s, %s, %s, %s)",
                    [i, linha, f"SKU-{rng.randint(1, 25):04d}", rng.randint(1, 5), round(rng.uniform(9, 250), 2)])


def _inserir_evento(con: psycopg.Connection, rng: random.Random) -> None:
    con.execute("INSERT INTO evento_site (ocorrido_em, pagina, visitante) VALUES (%s, %s, %s)",
                [INICIO + timedelta(seconds=rng.randint(0, 3600 * 24 * 200)), rng.choice(PAGINAS),
                 f"v{rng.randint(1, 120):04d}" if rng.random() > 0.1 else None])


def _contagens(con: psycopg.Connection) -> dict[str, int]:
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in ("cliente", "produto", "pedido", "item_pedido", "evento_site")}
