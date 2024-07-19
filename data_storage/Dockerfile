FROM python:3.10.9

COPY . /data_storage
WORKDIR /data_storage

EXPOSE 5000

RUN pip install -r ./data_storage/requirements.txt
CMD ["py","main.py"]
