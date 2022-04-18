FROM python:3.9-bullseye

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip==21.0

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

WORKDIR /app

ENV PATH /app:$PATH

USER root

EXPOSE 8000

RUN chmod -R 777 .