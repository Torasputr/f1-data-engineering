FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./fetch_bronze_race_and_sprint.py /app/fetch_bronze_race_and_sprint.py

CMD ["python", "/app/fetch_bronze_race_and_sprint.py"]