"""
Kafka Producer для отправки результатов работы модели.
"""
import json
import logging
import os
from typing import Any, Dict, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class ModelResultProducer:
    """Отправляет результаты предсказаний модели в Kafka."""

    def __init__(self):
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = os.getenv("KAFKA_TOPIC", "model-results")
        self.producer: Optional[KafkaProducer] = None
        self.bootstrap = bootstrap

    def _ensure_producer(self):
        """Ленивая инициализация — API стартует даже если Kafka недоступна."""
        if self.producer is None:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    linger_ms=10,
                )
                logger.info(f"Kafka Producer connected to {self.bootstrap}")
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}")
                raise

    def send_result(self, result: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Отправляет результат в Kafka.
        Возвращает True при успехе, False при ошибке (не роняет API).
        """
        try:
            self._ensure_producer()
            future = self.producer.send(self.topic, value=result, key=key)
            meta = future.get(timeout=10)
            logger.info(
                f"Sent to Kafka: topic={meta.topic}, "
                f"partition={meta.partition}, offset={meta.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Kafka error while sending: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while sending to Kafka: {e}")
            return False

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.producer = None


# Singleton
_producer_instance: Optional[ModelResultProducer] = None


def get_producer() -> ModelResultProducer:
    """Возвращает singleton-экземпляр Producer."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = ModelResultProducer()
    return _producer_instance