FROM python:3.7-alpine

WORKDIR /app

RUN apk update && apk add bash postgresql-dev gcc python3-dev musl-dev libxslt-dev

COPY requirements.txt /init/requirements.txt
RUN pip install -r /init/requirements.txt

COPY . /app