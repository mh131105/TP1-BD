"""
Utilitário de conexão ao banco de dados PostgreSQL.

Este módulo centraliza a criação de conexões sem utilizar um ORM. Usa a
biblioteca `psycopg`, que já está listada em `requirements.txt`.
"""

import psycopg
from psycopg import Connection


def get_conn(host: str, port: int, name: str, user: str, password: str) -> Connection:
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL.

    :param host: Host do banco (ex.: 'db')
    :param port: Porta do banco (ex.: 5432)
    :param name: Nome do banco de dados
    :param user: Usuário do banco
    :param password: Senha do banco
    :return: Instância de Connection
    """
    dsn = f"host={host} port={port} dbname={name} user={user} password={password}"
    return psycopg.connect(dsn)