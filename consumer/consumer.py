"""
Kafka Consumer для приёма результатов работы модели.
Читает сообщения из топика model-results и пишет в таблицу kafka_messages.
Конфигурация (БД + Kafka) берётся из Vault.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker

from vault_client import VaultClient

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("kafka-consumer")

# --- Получаем секреты из Vault ---
logger.info("Reading secrets from Vault...")
vault = VaultClient(
    vault_file=os.getenv("VAULT_FILE", "vault/secrets.enc"),
    password_file=os.getenv("VAULT_PASSWORD_FILE", "/nonexistent"),
)

db_config = vault.get_db_config()
DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]
DB_NAME = db_config["name"]
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]

kafka_config = vault.get_secret("kafka") or {}
KAFKA_BOOTSTRAP = kafka_config.get("bootstrap_servers", "kafka:29092")
KAFKA_TOPIC = kafka_config.get("topic", "model-results")
KAFKA_GROUP_ID = kafka_config.get("group_id", "model-results-consumer")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logger.info(f"DB config loaded: {DB_HOST}:{DB_PORT}/{DB_NAME} (user={DB_USER})")
logger.info(f"Kafka config loaded: {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}")
# --- SQLAlchemy модель ---
Base = declarative_base()


class KafkaMessage(Base):
    __tablename__ = "kafka_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    message_key = Column(String(255), nullable=True)
    payload = Column(Text, nullable=False)          # сырой JSON
    prediction = Column(Integer, nullable=True)     # распарсенные поля
    class_name = Column(String(100), nullable=True)
    confidence = Column(Float, nullable=True)
    received_at = Column(DateTime, default=datetime.utcnow, nullable=False)


def wait_for_db(engine, retries: int = 30, delay: int = 2):
    """Ждём доступности БД."""
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                logger.info("Database is ready")
                return True
        except Exception as e:
            logger.warning(f"DB not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Database is not available after retries")


def wait_for_kafka(retries: int = 30, delay: int = 3) -> KafkaConsumer:
    """Ждём доступности Kafka и создаём consumer."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: v.decode("utf-8") if v else None,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                consumer_timeout_ms=1000,   # чтобы можно было реагировать на Ctrl+C
            )
            logger.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}")
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Kafka is not available after retries")


def process_message(msg, SessionLocal):
    """Обработка одного сообщения: парсинг + сохранение в БД."""
    raw = msg.value
    parsed = {}
    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message JSON: {e}. Raw: {raw}")

    session = SessionLocal()
    try:
        record = KafkaMessage(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            message_key=msg.key,
            payload=raw or "",
            prediction=parsed.get("prediction"),
            class_name=parsed.get("class_name"),
            confidence=parsed.get("confidence"),
        )
        session.add(record)
        session.commit()
        logger.info(
            f"Saved message: topic={msg.topic} "
            f"partition={msg.partition} offset={msg.offset} "
            f"prediction={parsed.get('prediction')} "
            f"class_name={parsed.get('class_name')}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to save message to DB: {e}")
    finally:
        session.close()


def main():
    logger.info("Starting Kafka Consumer service...")
    logger.info(f"DB_URL (masked): postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # БД
    engine = create_engine(DB_URL, pool_pre_ping=True)
    wait_for_db(engine)

    # Создаём таблицу, если её ещё нет
    Base.metadata.create_all(engine)
    logger.info("Table kafka_messages is ready")

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    # Kafka
    consumer = wait_for_kafka()

    # Graceful shutdown
    stop = {"flag": False}

    def _shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Consuming messages. Press Ctrl+C to stop.")
    try:
        while not stop["flag"]:
            for msg in consumer:
                if stop["flag"]:
                    break
                process_message(msg, SessionLocal)
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed. Bye!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)