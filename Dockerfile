FROM python:3.10.2

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt
