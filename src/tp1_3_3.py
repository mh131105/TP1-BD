import argparse
import sys
import os
import csv
import time
from db import get_conn

def main() -> int:
    parser = argparse.ArgumentParser(description="TP1 3.3 – Executa consultas do dashboard no banco PostgreSQL.")
    parser.add_argument("--db-host", required=True, help="Host do banco (serviço Postgres no docker-compose).")
    parser.add_argument("--db-port", type=int, default=5432, help="Porta do banco (padrão 5432).")
    parser.add_argument("--db-name", required=True, help="Nome do banco de dados.")
    parser.add_argument("--db-user", required=True, help="Usuário do banco.")
    parser.add_argument("--db-pass", required=True, help="Senha do banco.")
    parser.add_argument("--product-asin", help="ASIN de um produto específico para consultas filtradas.")
    parser.add_argument("--output", default="/app/out", help="Diretório de saída dos CSVs (padrão: /app/out).")
    args = parser.parse_args()

    start_time = time.time()
    print("[Dashboard] Iniciando consultas analíticas...")

    # Conecta ao banco (usar autocommit=False para permitir transações, embora só leitura ocorra)
    try:
        conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass, autocommit=False)
    except Exception as e:
        print(f"[Dashboard] Erro ao conectar ao banco de dados: {e}", file=sys.stderr)
        return 1

    cur = conn.cursor()

    # Se product-asin fornecido, verificar existência e obter título (para uso nos outputs)
    product_title = None
    if args.product_asin:
        cur.execute("SELECT title FROM product WHERE asin = %s", (args.product_asin,))
        result = cur.fetchone()
        if not result:
            print(f"[Dashboard] Produto ASIN {args.product_asin} não encontrado no banco.", file=sys.stderr)
            conn.close()
            return 1
        product_title = result[0]

    # Garante que diretório de saída existe
    os.makedirs(args.output, exist_ok=True)

    try:
        # Consulta 1
        if args.product_asin:
            print(f"\nConsulta 1: 5 comentários mais úteis com maior e menor avaliação para o produto {args.product_asin} – {product_title}")
            # 5 reviews mais úteis com maior rating (5) e 5 com menor rating (1)
            cur.execute(
                """SELECT review_date, customer_id, rating, votes, helpful
                   FROM review
                   WHERE asin = %s AND rating = 5
                   ORDER BY helpful DESC
                   LIMIT 5""",
                (args.product_asin,)
            )
            top5_high = cur.fetchall()
            cur.execute(
                """SELECT review_date, customer_id, rating, votes, helpful
                   FROM review
                   WHERE asin = %s AND rating = 1
                   ORDER BY helpful DESC
                   LIMIT 5""",
                (args.product_asin,)
            )
            top5_low = cur.fetchall()

            # Imprime resumo no console
            print("  - Top 5 avaliações positivas mais úteis:")
            for (date, cust, rating, votes, helpful) in top5_high:
                print(f"    Cliente {cust} em {date}: rating {rating}, votos={votes}, útil={helpful}")
            print("  - Top 5 avaliações negativas mais úteis:")
            for (date, cust, rating, votes, helpful) in top5_low:
                print(f"    Cliente {cust} em {date}: rating {rating}, votos={votes}, útil={helpful}")

            # Salva CSVs
            with open(os.path.join(args.output, "q1_top5_reviews_pos.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["review_date", "customer_id", "rating", "votes", "helpful"])
                writer.writerows(top5_high)
            with open(os.path.join(args.output, "q1_top5_reviews_neg.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["review_date", "customer_id", "rating", "votes", "helpful"])
                writer.writerows(top5_low)
        else:
            print("\nConsulta 1: *Nenhum ASIN fornecido*, consulta ignorada.")

        # Consulta 2
        if args.product_asin:
            print(f"\nConsulta 2: Produtos similares a {args.product_asin} com melhor posição de vendas que ele")
            cur.execute(
                """SELECT p.asin, p.title, p.salesrank
                   FROM product_similar ps
                   JOIN product p ON ps.similar_asin = p.asin
                   JOIN product orig ON ps.asin = orig.asin
                   WHERE ps.asin = %s AND p.salesrank < orig.salesrank
                   ORDER BY p.salesrank ASC""",
                (args.product_asin,)
            )
            better_sales = cur.fetchall()
            for (asin, title, salesrank) in better_sales:
                print(f"    {asin} – {title} (salesrank {salesrank})")
            # CSV
            with open(os.path.join(args.output, "q2_similar_better_sales.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["asin", "title", "salesrank"])
                writer.writerows(better_sales)
        else:
            print("\nConsulta 2: *Nenhum ASIN fornecido*, consulta ignorada.")

        # Consulta 3
        if args.product_asin:
            print(f"\nConsulta 3: Evolução diária da média de avaliações para o produto {args.product_asin}")
            cur.execute(
                """SELECT review_date, AVG(rating) as avg_rating
                   FROM review
                   WHERE asin = %s
                   GROUP BY review_date
                   ORDER BY review_date""",
                (args.product_asin,)
            )
            daily_avg = cur.fetchall()
            print("    Data         Média Rating")
            for (date, avg) in daily_avg:
                avg_val = float(avg)
                print(f"    {date}   {avg_val:.2f}")
            # CSV
            with open(os.path.join(args.output, "q3_daily_avg_rating.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["date", "avg_rating"])
                for row in daily_avg:
                    writer.writerow([row[0], float(row[1])])
        else:
            print("\nConsulta 3: *Nenhum ASIN fornecido*, consulta ignorada.")

        # Consulta 4
        print("\nConsulta 4: Top 10 produtos líderes de venda em cada grupo")
        cur.execute(
            """SELECT grp, asin, title, salesrank FROM (
                 SELECT group_name as grp, asin, title, salesrank,
                        ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY salesrank ASC) as rk
                 FROM product
               ) sub
               WHERE rk <= 10
               ORDER BY grp, rk"""
        )
        top10_by_group = cur.fetchall()
        current_group = None
        rank = 1
        for (grp, asin, title, salesrank) in top10_by_group:
            if grp != current_group:
                # novo grupo
                current_group = grp
                rank = 1
                print(f"  Grupo: {grp}")
            print(f"    {rank}. {title} (ASIN {asin}) – salesrank {salesrank}")
            rank += 1
        # CSV
        with open(os.path.join(args.output, "q4_top10_sales_by_group.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["group", "asin", "title", "salesrank"])
            writer.writerows(top10_by_group)

        # Consulta 5
        print("\nConsulta 5: Top 10 produtos com maior média de avaliações úteis positivas")
        cur.execute(
            """SELECT p.asin, p.title, AVG(r.helpful::float) as avg_helpful
               FROM product p
               JOIN review r ON p.asin = r.asin
               WHERE r.rating >= 4
               GROUP BY p.asin, p.title
               ORDER BY avg_helpful DESC
               LIMIT 10"""
        )
        top10_helpful = cur.fetchall()
        for (asin, title, avg_helpful) in top10_helpful:
            print(f"    {title} (ASIN {asin}) – média útil = {avg_helpful:.2f}")
        # CSV
        with open(os.path.join(args.output, "q5_top10_avg_helpful_pos_reviews.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["asin", "title", "avg_helpful_positive_reviews"])
            for (asin, title, avg_helpful) in top10_helpful:
                writer.writerow([asin, title, float(avg_helpful)])

        # Consulta 6
        print("\nConsulta 6: Top 5 categorias com a maior média de avaliações úteis positivas por produto")
        cur.execute(
            """WITH prod_help AS (
                   SELECT asin, AVG(helpful::float) AS avg_help
                   FROM review
                   WHERE rating >= 4
                   GROUP BY asin
               )
               SELECT c.category_name, AVG(ph.avg_help) AS category_avg_help
               FROM category c
               JOIN product_category pc ON c.category_id = pc.category_id
               JOIN prod_help ph ON pc.asin = ph.asin
               GROUP BY c.category_id, c.category_name
               ORDER BY category_avg_help DESC
               LIMIT 5"""
        )
        top5_categories = cur.fetchall()
        for (cat_name, avg_help) in top5_categories:
            print(f"    {cat_name} – média útil positiva = {avg_help:.2f}")
        # CSV
        with open(os.path.join(args.output, "q6_top5_categories_avg_helpful_pos_reviews.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["category_name", "avg_helpful_positive_per_product"])
            for (cat_name, avg_help) in top5_categories:
                writer.writerow([cat_name, float(avg_help)])

        # Consulta 7
        print("\nConsulta 7: Top 10 clientes que mais fizeram comentários por grupo de produto")
        cur.execute(
            """SELECT grp, customer_id, review_count FROM (
                   SELECT p.group_name AS grp, r.customer_id, COUNT(*) AS review_count,
                          ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY COUNT(*) DESC) AS rk
                   FROM review r
                   JOIN product p ON r.asin = p.asin
                   GROUP BY p.group_name, r.customer_id
               ) sub
               WHERE rk <= 10
               ORDER BY grp, review_count DESC"""
        )
        top10_customers = cur.fetchall()
        current_group = None
        for (grp, customer_id, review_count) in top10_customers:
            if grp != current_group:
                current_group = grp
                print(f"  Grupo: {grp}")
            print(f"    Cliente {customer_id} – {review_count} comentários")
        # CSV
        with open(os.path.join(args.output, "q7_top10_customers_by_group.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["group", "customer_id", "review_count"])
            writer.writerows(top10_customers)

    except Exception as e:
        print(f"[Dashboard] Erro ao executar as consultas: {e}", file=sys.stderr)
        conn.close()
        return 1

    conn.close()
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"\n[Dashboard] Consultas concluídas com sucesso em {elapsed:.2f} segundos.")
    print(f"[Dashboard] Arquivos CSV de saída disponíveis em: {args.output}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
