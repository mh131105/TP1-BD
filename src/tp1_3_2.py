import argparse
import re
import sys
import time
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from db import get_conn


ReviewEntry = Dict[str, Optional[object]]
CategoryPath = List[Tuple[str, str]]
ProductData = Dict[str, object]
FlushFunction = Callable[[str], None]

BATCH_SIZE = 5000

SQL_INSERT_PRODUCT = (
    "INSERT INTO product (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (asin) DO UPDATE SET "
    "title = EXCLUDED.title, "
    "group_name = EXCLUDED.group_name, "
    "salesrank = EXCLUDED.salesrank, "
    "total_reviews = EXCLUDED.total_reviews, "
    "downloaded = EXCLUDED.downloaded, "
    "avg_rating = EXCLUDED.avg_rating"
)

SQL_INSERT_CATEGORY = (
    "INSERT INTO category (category_id, category_name, parent_id) VALUES (%s, %s, %s) "
    "ON CONFLICT (category_id) DO UPDATE SET "
    "category_name = EXCLUDED.category_name, "
    "parent_id = EXCLUDED.parent_id"
)

SQL_INSERT_PRODUCT_CATEGORY = (
    "INSERT INTO product_category (asin, category_id) VALUES (%s, %s) "
    "ON CONFLICT (asin, category_id) DO NOTHING"
)

SQL_INSERT_CUSTOMER = (
    "INSERT INTO customer (customer_id) VALUES (%s) "
    "ON CONFLICT (customer_id) DO NOTHING"
)

SQL_INSERT_REVIEW = (
    "INSERT INTO review (review_date, rating, votes, helpful, asin, customer_id) "
    "VALUES (%s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (asin, customer_id, review_date) DO NOTHING"
)

SQL_INSERT_SIMILAR = (
    "INSERT INTO product_similar (asin, similar_asin) VALUES (%s, %s) "
    "ON CONFLICT (asin, similar_asin) DO NOTHING"
)

INSERT_STATEMENTS: Dict[str, str] = {
    "product": SQL_INSERT_PRODUCT,
    "category": SQL_INSERT_CATEGORY,
    "product_category": SQL_INSERT_PRODUCT_CATEGORY,
    "customer": SQL_INSERT_CUSTOMER,
    "review": SQL_INSERT_REVIEW,
    "product_similar": SQL_INSERT_SIMILAR,
}

POST_LOAD_STATEMENTS: Tuple[str, ...] = (
    "ANALYZE",
)

TABLE_DEPENDENCIES: Dict[str, Tuple[str, ...]] = {
    "product_category": ("product", "category"),
    "review": ("product", "customer"),
    "product_similar": ("product",),
}

FINAL_FLUSH_ORDER: Tuple[str, ...] = (
    "product",
    "category",
    "customer",
    "review",
    "product_category",
    "product_similar",
)


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


def _ensure_product_defaults(
    product_data: ProductData,
) -> Optional[Tuple[str, str, Optional[str], Optional[int], Optional[int], Optional[int], Optional[float]]]:
    asin = product_data.get("asin")
    if not asin:
        return None

    discontinued = bool(product_data.get("discontinued"))

    title = product_data.get("title")
    if not title:
        title = "Discontinued product" if discontinued else "Unknown title"

    group_name = product_data.get("group")
    if not group_name:
        group_name = None if discontinued else "Unknown"

    salesrank_raw = product_data.get("salesrank")
    if discontinued:
        salesrank = salesrank_raw if isinstance(salesrank_raw, int) else None
    else:
        salesrank = _normalize_int(salesrank_raw)

    total_reviews_raw = product_data.get("total_reviews")
    if discontinued and total_reviews_raw is None:
        total_reviews = None
    else:
        total_reviews = _normalize_int(total_reviews_raw)

    downloaded_raw = product_data.get("downloaded")
    if discontinued and downloaded_raw is None:
        downloaded = None
    else:
        downloaded = _normalize_int(downloaded_raw)

    avg_rating_raw = product_data.get("avg_rating")
    if discontinued and avg_rating_raw is None:
        avg_rating = None
    else:
        avg_rating = _normalize_float(avg_rating_raw)

    return asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating


def _queue_with_flush(
    table: str,
    params: Tuple[object, ...],
    buffers: Dict[str, List[Tuple[object, ...]]],
    flush: "FlushFunction",
    dependencies: Dict[str, Tuple[str, ...]] = TABLE_DEPENDENCIES,
) -> None:
    buffer = buffers[table]
    buffer.append(params)
    if len(buffer) >= BATCH_SIZE:
        for dep_table in dependencies.get(table, ()):  # garante que dependências existam antes do commit
            flush(dep_table)
        flush(table)


def _create_flush_function(
    conn,
    cur,
    buffers: Dict[str, List[Tuple[object, ...]]],
    counts: Dict[str, int],
    flush_counters: Dict[str, int],
) -> FlushFunction:
    def _flush(table: str) -> None:
        batch = buffers[table]
        if not batch:
            return
        savepoint = f"sp_{table}_{flush_counters[table]}"
        flush_counters[table] += 1
        inserted_now = 0
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            cur.executemany(INSERT_STATEMENTS[table], batch)
            inserted_now = len(batch)
        except Exception as batch_exc:  # pragma: no cover - fallback path
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            print(
                f"[Carga][Aviso] Erro no lote da tabela {table}: {batch_exc}. "
                "Tentando inserções individuais...",
                file=sys.stderr,
            )
            for idx, params in enumerate(batch):
                row_savepoint = f"{savepoint}_r{idx}"
                cur.execute(f"SAVEPOINT {row_savepoint}")
                try:
                    cur.execute(INSERT_STATEMENTS[table], params)
                    cur.execute(f"RELEASE SAVEPOINT {row_savepoint}")
                    inserted_now += 1
                except Exception as row_exc:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {row_savepoint}")
                    print(
                        f"[Carga][Erro] Registro ignorado em {table}: {row_exc}. Valores: {params}",
                        file=sys.stderr,
                    )
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        else:
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")

        conn.commit()
        counts[table] += inserted_now
        buffers[table].clear()
        if inserted_now and counts[table] % BATCH_SIZE == 0:
            print(f"[Carga][Batch] {table}: {counts[table]} registros inseridos.")

    return _flush


def _insert_product(
    product_data: ProductData,
    buffers: Dict[str, List[Tuple[object, ...]]],
    flush: "FlushFunction",
    inserted_products: Set[str],
    pending_similars: Dict[str, Set[str]],
) -> None:
    product_defaults = _ensure_product_defaults(product_data)
    if not product_defaults:
        return

    asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating = product_defaults

    if asin not in inserted_products:
        inserted_products.add(asin)
        _queue_with_flush(
            "product",
            (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating),
            buffers,
            flush,
        )

    for path in product_data.get("categories", []):
        if not isinstance(path, Sequence) or not path:
            continue
        for idx, (cat_name, cat_id) in enumerate(path):
            parent_id = None
            if idx > 0:
                parent_raw = path[idx - 1][1]
                parent_id = int(parent_raw) if isinstance(parent_raw, str) and parent_raw.isdigit() else None
            cat_id_val = int(cat_id) if isinstance(cat_id, str) and cat_id.isdigit() else None
            if cat_id_val is None:
                continue
            _queue_with_flush(
                "category",
                (cat_id_val, cat_name, parent_id),
                buffers,
                flush,
            )
        leaf = path[-1][1] if path else None
        leaf_id = int(leaf) if isinstance(leaf, str) and leaf.isdigit() else None
        if leaf_id is not None:
            pair = (asin, leaf_id)
            _queue_with_flush("product_category", pair, buffers, flush)

    for sim in product_data.get("similar", []):
        if not sim:
            continue
        if sim == asin:
            continue
        if sim in inserted_products:
            pair = (asin, sim)
            _queue_with_flush("product_similar", pair, buffers, flush)
        else:
            pending_similars.setdefault(sim, set()).add(asin)

    waiting_sources = pending_similars.pop(asin, set())
    for source in waiting_sources:
        pair = (source, asin)
        if source in inserted_products:
            _queue_with_flush("product_similar", pair, buffers, flush)

    for rev in product_data.get("reviews", []):
        if not isinstance(rev, dict):
            continue
        cust_id = rev.get("customer")
        if not cust_id:
            continue
        _queue_with_flush("customer", (cust_id,), buffers, flush)
        rating = _normalize_int(rev.get("rating"))
        votes = _normalize_int(rev.get("votes"))
        helpful = _normalize_int(rev.get("helpful"))
        review_date = rev.get("date")
        if not review_date:
            continue
        _queue_with_flush(
            "review",
            (review_date, rating, votes, helpful, asin, cust_id),
            buffers,
            flush,
        )


def _new_product_data() -> ProductData:
    return {
        "asin": None,
        "title": None,
        "group": None,
        "salesrank": None,
        "total_reviews": None,
        "downloaded": None,
        "avg_rating": None,
        "discontinued": False,
        "categories": [],
        "similar": [],
        "reviews": [],
    }


def _apply_post_load_constraints(cur) -> None:
    for statement in POST_LOAD_STATEMENTS:
        cur.execute(statement)

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
        conn = get_conn(
            args.db_host,
            args.db_port,
            args.db_name,
            args.db_user,
            args.db_pass,
            autocommit=False,
        )
    except Exception as e:
        print(f"[Carga] Erro ao conectar ao banco de dados: {e}", file=sys.stderr)
        return 1

    cur = conn.cursor()
    try:
        cur.execute("SET synchronous_commit TO OFF")
        cur.execute("SET client_min_messages TO WARNING")
        conn.commit()
    except Exception as e:
        print(f"[Carga] Aviso ao ajustar sessão: {e}", file=sys.stderr)

    # Executa DDL do schema
    try:
        with open("/app/sql/schema.sql", "r") as schema_file:
            schema_sql = schema_file.read()
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]
            for stmt in statements:
                cur.execute(stmt)
        conn.commit()
        print("[Carga] Esquema do banco de dados criado com sucesso.")
    except Exception as e:
        print(f"[Carga] Erro ao criar o esquema do banco: {e}", file=sys.stderr)
        conn.close()
        return 1

    buffers: Dict[str, List[Tuple[object, ...]]] = {table: [] for table in INSERT_STATEMENTS}
    counts: Dict[str, int] = {table: 0 for table in INSERT_STATEMENTS}
    flush_counters: Dict[str, int] = {table: 0 for table in INSERT_STATEMENTS}

    flush = _create_flush_function(conn, cur, buffers, counts, flush_counters)

    # Inicializa estruturas auxiliares para evitar duplicatas
    inserted_products: Set[str] = set()
    pending_similars: Dict[str, Set[str]] = {}

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
                        _insert_product(
                            product_data,
                            buffers,
                            flush,
                            inserted_products,
                            pending_similars,
                        )
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
                    product_data["discontinued"] = True
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
                _insert_product(
                    product_data,
                    buffers,
                    flush,
                    inserted_products,
                    pending_similars,
                )

    except Exception as e:
        print(f"[Carga] Erro durante processamento do arquivo: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Flush final dos buffers
    for table in FINAL_FLUSH_ORDER:
        flush(table)

    try:
        _apply_post_load_constraints(cur)
        conn.commit()
    except Exception as e:
        print(f"[Carga] Erro ao aplicar constraints pós-carga: {e}", file=sys.stderr)
        conn.close()
        return 1

    conn.close()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"[Carga] Dados carregados com sucesso.")
    print(
        "[Carga] Totais inseridos -> Produtos: {prod}, Categorias: {cat}, Clientes: {cust}, "
        "Reviews: {rev}, Similaridades: {sim}.".format(
            prod=counts["product"],
            cat=counts["category"],
            cust=counts["customer"],
            rev=counts["review"],
            sim=counts["product_similar"],
        )
    )
    print(f"[Carga] Tempo total de execução: {elapsed:.2f} segundos.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
