FROM python:3.11-slim

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de requisitos e instale as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código-fonte
COPY src ./src

# Comando padrão: mostra a ajuda do script de consultas
CMD ["python", "src/tp1_3_3.py", "--help"]