FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir asyncpg hatchet-sdk
COPY listener/pg_listener.py /app/pg_listener.py
CMD ["python", "-u", "pg_listener.py"]
