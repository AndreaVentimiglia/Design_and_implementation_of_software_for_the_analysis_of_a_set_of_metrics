FROM python:3.10.9
COPY . /etl_data_pipeline
WORKDIR /etl_data_pipeline

RUN pip install -r ./etl_data_pipeline/requirements.txt

EXPOSE 5000
CMD ["py","main.py"]
