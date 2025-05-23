FROM jupyter/pyspark-notebook:latest

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /home/jovyan/work 