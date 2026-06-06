FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY system4/ ./system4/
COPY migrations/ ./migrations/

EXPOSE 8080

CMD ["uvicorn", "system4.main:app", "--host", "0.0.0.0", "--port", "8080"]
