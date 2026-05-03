FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY round_report.py /app/round_report.py
CMD ["python", "round_report.py"]

