import argparse
import sys
import time
from db import get_conn

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
    try:
        with open("/app/sql/schema.sql", "r") as schema_file:
            schema_sql = schema_file.read()
            cur.execute(schema_sql)
        print("[Carga] Esquema do banco de dados criado com sucesso.")
    except Exception as e:
        print(f"[Carga] Erro ao criar o esquema do banco: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Inicializa estruturas auxiliares para evitar duplicatas
    inserted_categories = set()   # IDs de categoria já inseridos
    inserted_customers = set()    # IDs de cliente já inseridos
    similar_pairs = []            # Armazena tuplas (asin, similar_asin) para inserir depois

    prod_count = cat_count = cust_count = rev_count = sim_count = 0

    # Processa o arquivo de entrada
    try:
        with open(args.input, "r", encoding="utf-8") as infile:
            product_data = {}  # dicionário para dados do produto atual em parsing
            for raw_line in infile:
                line = raw_line.strip()
                if not line:
                    continue  # ignora linhas vazias

                if line.startswith("Id:"):
                    # Início de um novo produto
                    if product_data:
                        # Finaliza inserções do produto anterior
                        asin = product_data.get("asin")
                        # Insere Produto
                        cur.execute(
                            "INSERT INTO product (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (asin, product_data.get("title"), product_data.get("group"), product_data.get("salesrank"),
                             product_data.get("total_reviews"), product_data.get("downloaded"), product_data.get("avg_rating"))
                        )
                        prod_count += 1
                        # Insere categorias (e hierarquia) e relação produto-categoria
                        for path in product_data.get("categories", []):
                            for idx, (cat_name, cat_id) in enumerate(path):
                                parent_id = None
                                if idx > 0:
                                    # parent_id = id da categoria anterior no caminho
                                    parent_id = path[idx - 1][1]
                                cat_id_val = int(cat_id) if cat_id.isdigit() else None
                                if cat_id_val is not None and cat_id_val not in inserted_categories:
                                    cur.execute(
                                        "INSERT INTO category (category_id, category_name, parent_id) VALUES (%s, %s, %s)",
                                        (cat_id_val, cat_name, parent_id)
                                    )
                                    inserted_categories.add(cat_id_val)
                                    cat_count += 1
                            # Liga o produto à categoria folha (última do caminho)
                            leaf_id = int(path[-1][1]) if path[-1][1].isdigit() else None
                            if leaf_id is not None:
                                cur.execute(
                                    "INSERT INTO product_category (asin, category_id) VALUES (%s, %s)",
                                    (asin, leaf_id)
                                )
                        # Armazena relações similares (inserção diferida)
                        for sim in product_data.get("similar", []):
                            similar_pairs.append((asin, sim))
                        # Insere reviews e clientes
                        for rev in product_data.get("reviews", []):
                            cust_id = rev.get("customer")
                            if cust_id and cust_id not in inserted_customers:
                                cur.execute(
                                    "INSERT INTO customer (customer_id) VALUES (%s)",
                                    (cust_id,)
                                )
                                inserted_customers.add(cust_id)
                                cust_count += 1
                            cur.execute(
                                "INSERT INTO review (review_date, rating, votes, helpful, asin, customer_id) VALUES (%s, %s, %s, %s, %s, %s)",
                                (rev.get("date"), rev.get("rating"), rev.get("votes"), rev.get("helpful"), asin, cust_id)
                            )
                            rev_count += 1

                    # Prepara um novo produto em parsing
                    product_data = {"asin": None, "title": None, "group": None, "salesrank": None,
                                    "total_reviews": None, "downloaded": None, "avg_rating": None,
                                    "categories": [], "similar": [], "reviews": []}

                elif line.startswith("ASIN:"):
                    product_data["asin"] = line.split(":", 1)[1].strip()
                elif line.startswith("title:"):
                    product_data["title"] = line.split(":", 1)[1].strip()
                elif line.startswith("group:"):
                    product_data["group"] = line.split(":", 1)[1].strip()
                elif line.startswith("salesrank:"):
                    val = line.split(":", 1)[1].strip()
                    product_data["salesrank"] = int(val) if val.isdigit() else None
                elif line.startswith("similar:"):
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            count_sim = int(parts[1])
                        except:
                            count_sim = 0
                        # pega os ASINs listados após o número
                        sim_asins = parts[2:2 + count_sim] if count_sim > 0 else parts[2:]
                        product_data["similar"] = sim_asins
                elif line.startswith("categories:"):
                    parts = line.split()
                    num_paths = int(parts[1]) if len(parts) > 1 else 0
                    for i in range(num_paths):
                        cat_line = infile.readline().strip()
                        if not cat_line:
                            continue
                        # Remove o '|' inicial e separa por '|'
                        if cat_line.startswith("|"):
                            cat_line = cat_line[1:]
                        segments = [seg for seg in cat_line.split("|") if seg]
                        path = []
                        for seg in segments:
                            # separa nome e id entre colchetes
                            if "[" in seg:
                                name, cid = seg.rsplit("[", 1)
                                cid = cid.rstrip("]")
                            else:
                                name, cid = seg, ""
                            path.append((name.strip(), cid))
                        product_data["categories"].append(path)
                elif line.startswith("reviews:"):
                    # Exemplo: "reviews: total: 8  downloaded: 8  avg rating: 4"
                    import re
                    m_total = re.search(r"total:\s*(\d+)", line)
                    m_down = re.search(r"downloaded:\s*(\d+)", line)
                    m_avg = re.search(r"avg rating:\s*([\d\.]+)", line)
                    total_reviews = int(m_total.group(1)) if m_total else 0
                    downloaded = int(m_down.group(1)) if m_down else 0
                    avg_rating = float(m_avg.group(1)) if m_avg else None
                    product_data["total_reviews"] = total_reviews
                    product_data["downloaded"] = downloaded
                    product_data["avg_rating"] = avg_rating
                    # Ler as próximas 'downloaded' linhas de reviews
                    for i in range(downloaded):
                        rev_line = infile.readline().strip()
                        if not rev_line:
                            continue
                        tokens = rev_line.split()
                        if len(tokens) >= 9:
                            date = tokens[0]
                            # tokens[1] deve ser "customer:" (possivelmente com colon junto)
                            cust_token = tokens[1]
                            customer_id = None
                            if cust_token.startswith("customer"):
                                # o ID do cliente pode estar no token[2] ou token[2] pode estar vazio se havia duplo espaço
                                customer_id = tokens[2] if tokens[2] != ":" else tokens[3]
                            # tokens[3] ou [4] é "rating:", seguido do valor
                            # tokens[5] "votes:", tokens[7] "helpful:"
                            try:
                                rating_idx = tokens.index("rating:")  # encontra índice do token "rating:"
                            except ValueError:
                                rating_idx = None
                            rating_val = int(tokens[rating_idx + 1]) if rating_idx else None
                            try:
                                votes_idx = tokens.index("votes:")
                            except ValueError:
                                votes_idx = None
                            votes_val = int(tokens[votes_idx + 1]) if votes_idx else None
                            try:
                                helpful_idx = tokens.index("helpful:")
                            except ValueError:
                                helpful_idx = None
                            helpful_val = int(tokens[helpful_idx + 1]) if helpful_idx else None
                            review_entry = {
                                "customer": customer_id,
                                "date": date,
                                "rating": rating_val,
                                "votes": votes_val,
                                "helpful": helpful_val
                            }
                            product_data["reviews"].append(review_entry)
                        # (Se len(tokens) < 9, ignora – formato inesperado)
            # Fim do loop: inserir o último produto lido (se existir)
            if product_data and product_data.get("asin"):
                asin = product_data["asin"]
                cur.execute(
                    "INSERT INTO product (asin, title, group_name, salesrank, total_reviews, downloaded, avg_rating) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (asin, product_data.get("title"), product_data.get("group"), product_data.get("salesrank"),
                     product_data.get("total_reviews"), product_data.get("downloaded"), product_data.get("avg_rating"))
                )
                prod_count += 1
                for path in product_data.get("categories", []):
                    for idx, (cat_name, cat_id) in enumerate(path):
                        parent_id = None
                        if idx > 0:
                            parent_id = path[idx - 1][1]
                        cat_id_val = int(cat_id) if cat_id.isdigit() else None
                        if cat_id_val is not None and cat_id_val not in inserted_categories:
                            cur.execute(
                                "INSERT INTO category (category_id, category_name, parent_id) VALUES (%s, %s, %s)",
                                (cat_id_val, cat_name, parent_id)
                            )
                            inserted_categories.add(cat_id_val)
                            cat_count += 1
                    leaf_id = int(path[-1][1]) if path[-1][1].isdigit() else None
                    if leaf_id is not None:
                        cur.execute(
                            "INSERT INTO product_category (asin, category_id) VALUES (%s, %s)",
                            (asin, leaf_id)
                        )
                for sim in product_data.get("similar", []):
                    similar_pairs.append((asin, sim))
                for rev in product_data.get("reviews", []):
                    cust_id = rev.get("customer")
                    if cust_id and cust_id not in inserted_customers:
                        cur.execute(
                            "INSERT INTO customer (customer_id) VALUES (%s)",
                            (cust_id,)
                        )
                        inserted_customers.add(cust_id)
                        cust_count += 1
                    cur.execute(
                        "INSERT INTO review (review_date, rating, votes, helpful, asin, customer_id) VALUES (%s, %s, %s, %s, %s, %s)",
                        (rev.get("date"), rev.get("rating"), rev.get("votes"), rev.get("helpful"), asin, cust_id)
                    )
                    rev_count += 1

    except Exception as e:
        print(f"[Carga] Erro durante processamento do arquivo: {e}", file=sys.stderr)
        conn.close()
        return 1

    # Insere as relações de similaridade coletadas
    try:
        for asin, sim_asin in similar_pairs:
            # Insere apenas se ambos asin e similar_asin existem na tabela product (FK garantirá isso)
            cur.execute(
                "INSERT INTO product_similar (asin, similar_asin) VALUES (%s, %s)",
                (asin, sim_asin)
            )
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
