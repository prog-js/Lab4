FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Зависимости
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код Consumer + VaultClient
COPY consumer/consumer.py .
COPY consumer/vault_client.py .

# Зашифрованный файл секретов
COPY vault/secrets.enc ./vault/secrets.enc

CMD ["python", "-u", "consumer.py"]