FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY standings.py /app/standings.py
CMD ["python", "standings.py"]