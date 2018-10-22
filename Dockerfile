FROM quay.io/keboola/docker-custom-python:latest

#COPY /data/ /data/

COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
