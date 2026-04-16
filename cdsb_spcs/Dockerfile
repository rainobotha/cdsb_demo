FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    snowflake-connector-python \
    curl_cffi

COPY scraper.py .

CMD ["python", "scraper.py"]
