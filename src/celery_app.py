import os
from celery import Celery

REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")

app = Celery('law_extractor', broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    task_acks_late=True,
    worker_prefetch_multiplier=1, # Ensures one task per worker at a time for stability
    task_reject_on_worker_lost=True
)