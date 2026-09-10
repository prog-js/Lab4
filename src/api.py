from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
import os
import jwt
from datetime import datetime, timedelta

from src.db import get_db, Prediction, ModelMetric, init_db
from src.analytics import GraduateAnalytics

from src.kafka_producer import get_producer

import logging
logger = logging.getLogger(__name__)

# Секретный ключ для JWT (из переменных окружения)
SECRET_KEY = os.environ.get('API_SECRET_KEY', 'fallback-key-change-me')
ALGORITHM = "HS256"

app = FastAPI(title="Graduate Analytics API with PostgreSQL")
security = HTTPBearer()

# Инициализация БД при старте
@app.on_event("startup")
def startup():
    init_db()

# Аналитика
analytics = GraduateAnalytics()

# --- Модели Pydantic для запросов ---
class PredictionRequest(BaseModel):
    features: List[float]

class PredictionResponse(BaseModel):
    prediction: int
    class_name: str
    confidence: float
    saved_to_db: bool

# --- Функция для создания JWT токена (для тестирования) ---
def create_test_token(user_id: str = "test_user"):
    payload = {
        "sub": user_id,
        "exp": datetime.utcnow() + timedelta(hours=1)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

# --- Middleware для проверки токена ---
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except jwt.PyJWTError:
        raise HTTPException(status_code=403, detail="Invalid authentication token")

# --- Эндпоинты ---
@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/token")
def get_token(user_id: str = "test_user"):
    """Эндпоинт для получения тестового токена"""
    token = create_test_token(user_id)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/predict", response_model=PredictionResponse)
def predict(
    request: PredictionRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(verify_token)
):
    """..."""
    import random
    prediction = random.randint(0, 2)
    confidence = random.random()
    class_names = {0: "setosa", 1: "versicolor", 2: "virginica"}

    # Сохранение в БД
    db_prediction = Prediction(
        input_features=request.features,
        prediction=prediction,
        confidence=confidence,
        user_id=user_id
    )
    db.add(db_prediction)
    db.commit()
    db.refresh(db_prediction)  # ← получить id после commit

    class_name = class_names.get(prediction, "unknown")

    # Отправка результата в Kafka
    try:
        producer = get_producer()
        kafka_payload = {
            "prediction_id": db_prediction.id,
            "user_id": user_id,
            "input_features": request.features,
            "prediction": prediction,
            "class_name": class_name,
            "confidence": confidence,
        }
        producer.send_result(kafka_payload, key=str(db_prediction.id))
    except Exception as e:
        # Kafka не должна ломать основной флоу
        logger.error(f"Kafka sending failed: {e}")

    return PredictionResponse(
        prediction=prediction,
        class_name=class_name,
        confidence=confidence,
        saved_to_db=True
    )

@app.post("/metrics")
def save_metric(
    metric_name: str,
    metric_value: float,
    model_version: str = "1.0",
    db: Session = Depends(get_db),
    user_id: str = Depends(verify_token)
):
    """Сохранение метрик модели в БД"""
    metric = ModelMetric(
        metric_name=metric_name,
        metric_value=metric_value,
        model_version=model_version
    )
    db.add(metric)
    db.commit()
    return {"status": "metric saved"}

@app.get("/metrics/{metric_name}")
def get_metrics(
    metric_name: str,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: str = Depends(verify_token)
):
    """Получение истории метрик из БД"""
    metrics = db.query(ModelMetric).filter(
        ModelMetric.metric_name == metric_name
    ).order_by(ModelMetric.timestamp.desc()).limit(limit).all()
    return {"metrics": [{"value": m.metric_value, "timestamp": m.timestamp} for m in metrics]}

# ... остальные эндпоинты аналитики (regions, universities, top_specialties_matrix и т.д.)