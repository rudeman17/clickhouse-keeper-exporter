FROM python:3.10-slim

WORKDIR /app

COPY keeper_exporter.py ./
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9141

ENTRYPOINT ["python3", "keeper_exporter.py"]