FROM python:3.10.9

COPY . /data_retrieval
WORKDIR /data_retrieval

EXPOSE 5000

RUN pip install -r ./data_retrieval/requirements.txt
CMD ["py","main.py"]
