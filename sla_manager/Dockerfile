FROM python:3.10.9
COPY . /sla_manager
WORKDIR /sla_manager

RUN pip install -r ./sla_manager/requirements.txt

EXPOSE 5000
CMD ["py","main.py"]
