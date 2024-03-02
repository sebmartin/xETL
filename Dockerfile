FROM python:3.12-alpine

WORKDIR /app

RUN apk update && apk add bash postgresql-dev gcc python3-dev musl-dev libxslt-dev
RUN pip install poetry==1.4.2

COPY pyproject.toml poetry.lock /app/
RUN poetry install --with dev

COPY . /app