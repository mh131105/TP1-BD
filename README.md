#TP1-BD
docker compose up -d --build
docker compose ps   # (opcional) ver se o Postgres ficou healthy

docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --input /data/snap_amazon.txt

Confere que o arquivo snap_amazon.txt está em ./data do teu repo — esse diretório é montado no contêiner em /data.

# Exemplo usando um ASIN real do dataset (1559362022)
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --product-asin 1559362022 \
  --output /app/out
docker compose down -v

docker exec -it tp1-db-1 psql -U postgres -d ecommerce
