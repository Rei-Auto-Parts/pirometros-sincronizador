#!/usr/bin/env python3
"""
Sincronizador de pirômetros
Copia as leituras novas da tabela Receiver_Data (SQL Server da RAPFUF10)
para a tabela receiver_data no Postgres da VPS.

Fica rodando sempre: sincroniza, espera INTERVALO_SEGUNDOS (padrão 300) e repete.
Se a RAPFUF10 ou o Postgres estiverem fora do ar, registra o erro e tenta no próximo ciclo.
Configuração por variáveis de ambiente (no Easypanel) ou num arquivo .env na mesma pasta.
"""
import logging
import os
import sys
import time

import pg8000.native
import pytds

LOTE = 5000  # máximo de linhas copiadas por consulta

COLUNAS = [
    "id", "gtw_id", "gtw_name", "dev_id", "dev_name", "reg_date",
    "reg_thermocouple", "reg_unit", "reg_tcj", "reg_read_delay",
    "reg_place", "reg_battery", "reg_temperature", "reg_detect_time",
]

CRIAR_TABELA = [
    """
    CREATE TABLE IF NOT EXISTS receiver_data (
        id               integer PRIMARY KEY,
        gtw_id           varchar(14),
        gtw_name         varchar(6),
        dev_id           varchar(14),
        dev_name         varchar(6),
        reg_date         timestamp,          -- horário local, igual ao SQL Server
        reg_thermocouple varchar(6),
        reg_unit         varchar(10),
        reg_tcj          double precision,
        reg_read_delay   integer,
        reg_place        integer,
        reg_battery      integer,
        reg_temperature  integer,
        reg_detect_time  double precision,
        synced_at        timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_receiver_data_date ON receiver_data (reg_date)",
    "CREATE INDEX IF NOT EXISTS idx_receiver_data_dev ON receiver_data (dev_id, reg_date)",
]

INSERIR = (
    f"INSERT INTO receiver_data ({', '.join(COLUNAS)}) "
    f"VALUES ({', '.join(':' + c for c in COLUNAS)}) "
    "ON CONFLICT (id) DO NOTHING"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sincronizador")
logging.getLogger("pytds").setLevel(logging.WARNING)  # esconde o log técnico do driver


def carregar_env(caminho):
    """Lê o .env (CHAVE=valor) sem precisar de biblioteca extra."""
    if not os.path.exists(caminho):
        return
    with open(caminho, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def cfg(nome, padrao=None):
    valor = os.environ.get(nome, padrao)
    if valor is None:
        log.error("Faltando a configuração %s no .env", nome)
        sys.exit(2)
    return valor


def sincronizar():
    pg = pg8000.native.Connection(
        host=cfg("PG_HOST"),
        port=int(cfg("PG_PORT", "5432")),
        database=cfg("PG_DATABASE"),
        user=cfg("PG_USER"),
        password=cfg("PG_PASSWORD"),
        timeout=30,
    )
    try:
        for sql in CRIAR_TABELA:
            pg.run(sql)

        ultimo_id = pg.run("SELECT COALESCE(MAX(id), 0) FROM receiver_data")[0][0]

        ms = pytds.connect(
            dsn=cfg("MSSQL_HOST"),
            port=int(cfg("MSSQL_PORT", "1433")),
            database=cfg("MSSQL_DATABASE"),
            user=cfg("MSSQL_USER"),
            password=cfg("MSSQL_PASSWORD"),
            login_timeout=15,
            timeout=60,
            autocommit=True,
        )
        with ms, ms.cursor() as cur:
            # Proteção: se a tabela do fornecedor for recriada, os ids voltam a 1
            cur.execute("SELECT MAX(id) FROM Receiver_Data")
            max_origem = cur.fetchone()[0] or 0
            if max_origem < ultimo_id:
                log.warning(
                    "ATENÇÃO: maior id no SQL Server (%s) é menor que o do Postgres (%s). "
                    "A tabela do fornecedor pode ter sido recriada. Nada foi copiado.",
                    max_origem, ultimo_id,
                )
                return

            total = 0
            while True:
                cur.execute(
                    f"SELECT TOP {LOTE} {', '.join(COLUNAS)} "
                    "FROM Receiver_Data WHERE id > %s ORDER BY id",
                    (ultimo_id,),
                )
                linhas = cur.fetchall()
                if not linhas:
                    break

                pg.run("START TRANSACTION")
                try:
                    for linha in linhas:
                        pg.run(INSERIR, **dict(zip(COLUNAS, linha)))
                    pg.run("COMMIT")
                except Exception:
                    pg.run("ROLLBACK")
                    raise

                total += len(linhas)
                ultimo_id = linhas[-1][0]
                if len(linhas) < LOTE:
                    break

        log.info("OK: %s leitura(s) nova(s) copiada(s). Último id: %s", total, ultimo_id)
    finally:
        pg.close()


def main():
    carregar_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    intervalo = int(cfg("INTERVALO_SEGUNDOS", "300"))
    log.info("Sincronizador iniciado. Intervalo: %s segundos", intervalo)
    while True:
        try:
            sincronizar()
        except Exception as erro:
            log.error("Falha na sincronização (tento de novo no próximo ciclo): %s", erro)
        time.sleep(intervalo)


if __name__ == "__main__":
    main()
