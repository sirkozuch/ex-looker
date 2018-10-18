FROM quay.io/keboola/docker-custom-python:latest

RUN pip install  --upgrade --no-cache-dir --ignore-installed pylooker

COPY . /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
