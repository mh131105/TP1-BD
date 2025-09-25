import argparse
import os
import re
import sys
import time
from typing import Dict, List, Optional, Sequence, Set, Tuple

from db import get_conn


ReviewEntry = Dict[str, Optional[object]]
CategoryPath = List[Tuple[str, str]]
ProductData = Dict[str, object]


REVIEW_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{1,2}-\d{1,2})\s+"
    r"(?:customer|cutomer):\s*(?P<customer>\S+)\s+"
    r"rating:\s*(?P<rating>\d+)\s+"
    r"votes:\s*(?P<votes>\d+)\s+"
    r"helpful:\s*(?P<helpful>\d+)"
    r"$",
    re.IGNORECASE,
)


def _normalize_int(value: Optional[object], default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_float(value: Optional[object], default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_review_line(raw_line: str) -> Optional[ReviewEntry]:
    match = REVIEW_PATTERN.match(raw_line.strip())
    if not match:
        return None
    groups = match.groupdict()
    return {
        "date": groups.get("date"),
        "customer": groups.get("customer"),
        "rating": _normalize_int(groups.get("rating")),
        "votes": _normalize_int(groups.get("votes")),
        "helpful": _normalize_int(groups.get("helpful")),
    }


def _ensure_product_defaults(product_data: ProductData) -> Optional[Tuple[str, str, str, int, int, int, float]]:
    asin = product_data.get("asin")
    if not asin:
        return None

    title = product_data.get("title")
    if not title:
        title = "Unknown title"

    group_name = product_data.get("group")
    if not group_name:
        group_name = "Unknown"

    salesrank = _normalize_int(product_data.get("salesrank"))
    total_reviews = _normalize_int(product_data.get("total_reviews"))
    downloaded = _normalize_int(product_data.get("downloaded"))
    avg_rating = _normalize_float(product_data.get("avg_rating"))

    return asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating


def _insert_product(
    cur,
    product_data: ProductData,
    inserted_products: Set[str],
    inserted_categories: set,
    inserted_customers: set,
    similar_pairs: List[Tuple[str, str]],
) -> Tuple[int, int, int, int]:
    product_defaults = _ensure_product_defaults(product_data)
    if not product_defaults:
        return 0, 0, 0, 0

    asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating = product_defaults

    cur.execute(
        "INSERT INTO product (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating),
    )
    inserted_products.add(asin)

    prod_inc = 1
    cat_inc = 0
    cust_inc = 0
    rev_inc = 0

    for path in product_data.get("categories", []):
        if not isinstance(path, Sequence) or not path:
            continue
        for idx, (cat_name, cat_id) in enumerate(path):
            parent_id = None
            if idx > 0:
                parent_raw = path[idx - 1][1]
                parent_id = int(parent_raw) if isinstance(parent_raw, str) and parent_raw.isdigit() else None
            cat_id_val = int(cat_id) if isinstance(cat_id, str) and cat_id.isdigit() else None
            if cat_id_val is not None and cat_id_val not in inserted_categories:
                cur.execute(
                    "INSERT INTO category (category_id, category_name, parent_id) VALUES (%s, %s, %s)",
                    (cat_id_val, cat_name, parent_id),
                )
                inserted_categories.add(cat_id_val)
                cat_inc += 1
        leaf = path[-1][1] if path else None
        leaf_id = int(leaf) if isinstance(leaf, str) and leaf.isdigit() else None
        if leaf_id is not None:
            cur.execute(
                "INSERT INTO product_category (asin, category_id) VALUES (%s, %s)",
                (asin, leaf_id),
            )

    for sim in product_data.get("similar", []):
        if sim:
            similar_pairs.append((asin, sim))

    for rev in product_data.get("reviews", []):
        if not isinstance(rev, dict):
            continue
        cust_id = rev.get("customer")
        if not cust_id:
            continue
        if cust_id not in inserted_customers:
            cur.execute(
                "INSERT INTO customer (customer_id) VALUES (%s)",
                (cust_id,),
            )
            inserted_customers.add(cust_id)
            cust_inc += 1
        rating = _normalize_int(rev.get("rating"))
        votes = _normalize_int(rev.get("votes"))
        helpful = _normalize_int(rev.get("helpful"))
        review_date = rev.get("date")
        if not review_date:
            continue
        cur.execute(
            "INSERT INTO review (review_date, rating, votes, helpful, asin, customer_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (review_date, rating, votes, helpful, asin, cust_id),
        )
        rev_inc += 1

    return prod_inc, cat_inc, cust_inc, rev_inc


def _new_product_data() -> ProductData:
    return {
        "asin": None,
        "title": None,
        "group": None,
        "salesrank": None,
        "total_reviews": None,
        "downloaded": None,
        "avg_rating": None,
        "categories": [],
        "similar": [],
        "reviews": [],
    }

def main() -> int:
    parser = argparse.ArgumentParser(description="Script de carga (TP1 3.2) – cria o esquema e carrega dados no PostgreSQL.")
    # Parâmetros de conexão (obrigatórios, exceto porta)
    parser.add_argument("--db-host", type=str, required=True, help="Hostname do servidor de banco (ex: 'db').")
    parser.add_argument("--db-port", type=int, default=5432, help="Porta do servidor Postgres (padrão 5432).")
    parser.add_argument("--db-name", type=str, required=True, help="Nome do banco de dados.")
    parser.add_argument("--db-user", type=str, required=True, help="Usuário do banco de dados.")
    parser.add_argument("--db-pass", type=str, required=True, help="Senha do banco de dados.")
    # Parâmetro do caminho do arquivo de entrada
    parser.add_argument("--input", type=str, required=True, help="Caminho do arquivo de entrada SNAP dentro do contêiner (ex: /data/snap_amazon.txt).")

    args = parser.parse_args()
    start_time = time.time()
    print(f"[Carga] Iniciando carregamento do arquivo: {args.input}")

    # Conecta ao banco
    try:
        # Usa autocommit=True para simplificar inserções em lote (evita necessidade de commits manuais)
        conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass, autocommit=True)
    except Exception as e:
        print(f"[Carga] Erro ao conectar ao banco de dados: {e}", file=sys.stderr)
        return 1

    cur = conn.cursor()
    # Executa DDL do schema
    base_dir = os.path.dirname(os.path.abspath(__file__))
    schema_candidates = [
        "/app/sql/schema.sql",
        os.path.join(base_dir, "..", "sql", "schema.sql"),
        os.path.join(os.getcwd(), "sql", "schema.sql"),
    ]
    schema_path = next((path for path in schema_candidates if os.path.exists(path)), None)

    if not schema_path:
        print("[Carga] Arquivo de schema não encontrado.", file=sys.stderr)
        conn.close()
        return 1

    try:
        with open(schema_path, "r") as schema_file:
            schema_sql = schema_file.read()
            cur.execute(schema_sql)
        print("[Carga] Esquema do banco de dados criado com sucesso.")
    except Exception as e:
        print(f"[Carga] Erro ao criar o esquema do banco: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Inicializa estruturas auxiliares para evitar duplicatas
    inserted_products: Set[str] = set()  # ASINs já persistidos
    inserted_categories = set()         # IDs de categoria já inseridos
    inserted_customers = set()          # IDs de cliente já inseridos
    similar_pairs = []                  # Armazena tuplas (asin, similar_asin) para inserir depois

    prod_count = cat_count = cust_count = rev_count = sim_count = 0

    # Processa o arquivo de entrada
    try:
        with open(args.input, "r", encoding="utf-8") as infile:
            product_data: Optional[ProductData] = None
            for raw_line in infile:
                line = raw_line.strip()
                if not line:
                    continue  # ignora linhas vazias

                if line.startswith("Id:"):
                    if product_data:
                        prod_inc, cat_inc, cust_inc, rev_inc = _insert_product(
                            cur,
                            product_data,
                            inserted_products,
                            inserted_categories,
                            inserted_customers,
                            similar_pairs,
                        )
                        prod_count += prod_inc
                        cat_count += cat_inc
                        cust_count += cust_inc
                        rev_count += rev_inc
                    product_data = _new_product_data()
                    continue

                if product_data is None:
                    continue

                if line.startswith("ASIN:"):
                    product_data["asin"] = line.split(":", 1)[1].strip()
                elif line.startswith("title:"):
                    product_data["title"] = line.split(":", 1)[1].strip()
                elif line.lower().startswith("discontinued product"):
                    product_data["title"] = "Discontinued product"
                elif line.startswith("group:"):
                    product_data["group"] = line.split(":", 1)[1].strip()
                elif line.startswith("salesrank:"):
                    val = line.split(":", 1)[1].strip()
                    product_data["salesrank"] = _normalize_int(val)
                elif line.startswith("similar:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        count_sim = _normalize_int(parts[1])
                        sim_asins = parts[2:2 + count_sim] if count_sim > 0 else parts[2:]
                        product_data["similar"] = sim_asins
                elif line.startswith("categories:"):
                    parts = line.split()
                    num_paths = _normalize_int(parts[1]) if len(parts) > 1 else 0
                    for _ in range(num_paths):
                        cat_line = infile.readline()
                        if not cat_line:
                            break
                        cat_line = cat_line.strip()
                        if not cat_line:
                            continue
                        if cat_line.startswith("|"):
                            cat_line = cat_line[1:]
                        segments = [seg for seg in cat_line.split("|") if seg]
                        path: CategoryPath = []
                        for seg in segments:
                            if "[" in seg:
                                name, cid = seg.rsplit("[", 1)
                                cid = cid.rstrip("]")
                            else:
                                name, cid = seg, ""
                            path.append((name.strip(), cid))
                        if path:
                            product_data["categories"].append(path)
                elif line.startswith("reviews:"):
                    m_total = re.search(r"total:\s*(\d+)", line)
                    m_down = re.search(r"downloaded:\s*(\d+)", line)
                    m_avg = re.search(r"avg rating:\s*([\d\.]+)", line)
                    product_data["total_reviews"] = int(m_total.group(1)) if m_total else None
                    product_data["downloaded"] = int(m_down.group(1)) if m_down else None
                    product_data["avg_rating"] = float(m_avg.group(1)) if m_avg else None
                    num_to_read = _normalize_int(product_data.get("downloaded"))
                    for _ in range(num_to_read):
                        rev_line = infile.readline()
                        if not rev_line:
                            break
                        review_entry = _parse_review_line(rev_line)
                        if review_entry:
                            product_data["reviews"].append(review_entry)
            if product_data:
                prod_inc, cat_inc, cust_inc, rev_inc = _insert_product(
                    cur,
                    product_data,
                    inserted_products,
                    inserted_categories,
                    inserted_customers,
                    similar_pairs,
                )
                prod_count += prod_inc
                cat_count += cat_inc
                cust_count += cust_inc
                rev_count += rev_inc

    except Exception as e:
        print(f"[Carga] Erro durante processamento do arquivo: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Garante que o conjunto de ASINs reflita exatamente o que foi persistido no banco
    try:
        cur.execute("SELECT asin FROM product")
        persisted_asins: Set[str] = {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[Carga] Erro ao consultar produtos persistidos: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Insere as relações de similaridade coletadas
    try:
        seen_pairs: Set[Tuple[str, str]] = set()
        for asin, sim_asin in similar_pairs:
            if asin == sim_asin:
                continue
            if asin not in persisted_asins or sim_asin not in persisted_asins:
                continue
            pair = (asin, sim_asin)
            if pair in seen_pairs:
                continue
            cur.execute(
                "INSERT INTO product_similar (asin, similar_asin) VALUES (%s, %s)",
                pair,
            )
            seen_pairs.add(pair)
            sim_count += 1
    except Exception as e:
        print(f"[Carga] Erro ao inserir relações similares: {e}", file=sys.stderr)
        conn.close()
        return 1

    conn.close()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"[Carga] Dados carregados com sucesso.")
    print(f"[Carga] Totais inseridos -> Produtos: {prod_count}, Categorias: {cat_count}, Clientes: {cust_count}, Reviews: {rev_count}, Similaridades: {sim_count}.")
    print(f"[Carga] Tempo total de execução: {elapsed:.2f} segundos.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
