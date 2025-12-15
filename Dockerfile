FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY drime_s3/ ./drime_s3/

RUN pip install --no-cache-dir .

ENV DRIME_API_KEY=""

EXPOSE 8081

CMD ["python", "-m", "drime_s3", "--host", "0.0.0.0"]
