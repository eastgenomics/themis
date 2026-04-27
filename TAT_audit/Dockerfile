FROM python:3.13.13-slim-bookworm

ENV TZ=Europe/London

RUN apt-get update && apt-get install -y curl jq nano

COPY . /app/

RUN pip install -r /app/requirements.txt

WORKDIR /app/
