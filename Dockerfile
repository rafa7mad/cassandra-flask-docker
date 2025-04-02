FROM python:3.11-slim

WORKDIR /app

COPY . .

COPY secure-connect-test-cassandra.zip .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "app.py"]
