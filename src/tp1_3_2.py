"""
Script de ETL para o Trabalho Prático 1.

Este módulo estabelece uma conexão com o banco PostgreSQL, cria o schema
e carrega os dados do arquivo de entrada. Por enquanto contém apenas a
estrutura básica de parsing de argumentos e conexão; a lógica de ETL
será implementada nas próximas etapas.
"""

import argparse
import sys
from db import get_conn


def parse_args() -> argparse.Namespace:
    """
    Define e processa os argumentos de linha de comando.

    :return: Namespace com os parâmetros fornecidos
    """
    parser = argparse.ArgumentParser(
        description="TP1 3.2 – cria esquema e carrega dados (ETL)."
    )
    parser.add_argument(
        "--db-host", required=True, help="Host do banco (serviço do Postgres)."
    )
    parser.add_argument(
        "--db-port", type=int, default=5432, help="Porta do banco."
    )
    parser.add_argument(
        "--db-name", required=True, help="Nome do banco de dados."
    )
    parser.add_argument(
        "--db-user", required=True, help="Usuário do banco."
    )
    parser.add_argument(
        "--db-pass", required=True, help="Senha do banco."
    )
    parser.add_argument(
        "--input", required=True, help="Caminho para o arquivo de dados (no contêiner)."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("[ETL] conectando ao banco…")
    try:
        with get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass) as conn:
            # Aqui você deverá criar as tabelas e carregar os dados do arquivo.
            print("[ETL] conexão estabelecida. Próximas etapas: criar tabelas e carregar dados.")
        return 0
    except Exception as e:
        print(f"[ETL] erro: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())