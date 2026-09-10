# Базовый образ с Python
FROM python:3.9-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код и конфигурацию (без больших файлов)
COPY src/ ./src/
COPY config.ini .

# Копируем зашифрованный файл с секретами
COPY vault/secrets.enc ./vault/

# Копируем данные (CSV файлы)
COPY data/ ./data/

# Открываем порт
EXPOSE 8000

# Запуск API
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]