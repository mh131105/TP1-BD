# TP1-BD

Este repositório contém a estrutura necessária para construir o ambiente Docker, carregar o esquema/dados e executar as consultas do dashboard. Siga exatamente os passos abaixo.

## Pré-requisito: baixar o dataset SNAP
```bash
mkdir -p data
curl -L https://snap.stanford.edu/data/amazon-meta.txt.gz -o data/snap_amazon.txt.gz
gunzip -c data/snap_amazon.txt.gz > data/snap_amazon.txt
rm data/snap_amazon.txt.gz
```
* Faz o download do arquivo original `amazon-meta.txt.gz` do SNAP (Stanford Network Analysis Project) e o descompacta para `data/snap_amazon.txt`.
* Caso já possua o dataset localmente, basta copiá-lo para `./data/snap_amazon.txt`.
* Confirme que o arquivo resultante está presente antes de prosseguir para a etapa 2.

## 1) Construir e subir os serviços
```
docker compose up -d --build
```
* Constrói as imagens definidas no `docker-compose.yml` e sobe os contêineres em segundo plano.
* Aguarde até o serviço `db` ficar com status **healthy** (`docker compose ps`) antes de continuar.

## 2) Criar esquema e carregar dados
```
docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --input /data/snap_amazon.txt
```
* Executa o script `tp1_3.2.py` dentro do contêiner `app` para criar todas as tabelas necessárias e inserir os dados.
* O diretório local `./data` é montado como `/data` no contêiner: confirme que `snap_amazon.txt` está presente ali antes de rodar o comando.
* A flag `--rm` garante que o contêiner efêmero seja removido ao final da execução.

## 3) Executar o Dashboard (todas as consultas)
```
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --output /app/out
```
* Roda o script `tp1_3.3.py`, que consulta o banco e gera os relatórios do dashboard.
* Os resultados são gravados no diretório `./out` do host (montado como `/app/out` dentro do contêiner `app`).

Quando terminar, utilize `docker compose down -v` para derrubar os serviços e limpar os volumes, caso necessário.
