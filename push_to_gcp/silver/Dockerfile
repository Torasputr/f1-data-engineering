FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bronze_to_silver.py /app/bronze_to_silver.py
CMD ["python", "bronze_to_silver.py"]