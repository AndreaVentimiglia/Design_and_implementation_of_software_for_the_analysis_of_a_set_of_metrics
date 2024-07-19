FROM python:3.10.9

COPY . /db_manager
WORKDIR /db_manager

COPY requirements.txt .
RUN pip install -r ./db_manager/requirements.txt

COPY . .

CMD ["py","main.py"]
