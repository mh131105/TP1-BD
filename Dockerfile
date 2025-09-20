# Usa a imagem base oficial do Python 3.11 slim
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências para o diretório de trabalho
COPY requirements.txt .

# Instala as dependências Python listadas em requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código-fonte da aplicação para o contêiner
COPY src/ ./src/
COPY sql/ ./sql/

# Define o comando padrão do contêiner (mostra ajuda do script de dashboard)
CMD ["python", "src/tp1_3.3.py", "--help"]
