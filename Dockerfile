FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps

COPY . .

# Ensure PYTHONPATH is set to find the src module
ENV PYTHONPATH=/app