FROM python:3.11

ARG POETRY_VERSION=1.7.1

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY .env .env
COPY pyproject.toml  pyproject.toml
COPY poetry.lock  poetry.lock


RUN pip3 install --no-cache-dir poetry==${POETRY_VERSION} \
    && poetry env use 3.11 \
    && poetry install --no-cache


ENTRYPOINT [ "poetry", "run", "python", "ingest_data.py" ]
