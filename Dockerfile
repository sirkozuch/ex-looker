FROM quay.io/keboola/docker-custom-python:latest

RUN pip install  --upgrade --no-cache-dir --ignore-installed logging_gelf

#COPY /data/ /data/

COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]