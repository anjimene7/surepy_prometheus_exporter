FROM python:3.10-bullseye

WORKDIR /opt/app

ADD ./requirements.txt /opt/app/requirements.txt

RUN apt-get update -y \
    && apt-get upgrade -y \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install -r requirements.txt

ADD ./main.py /opt/app

CMD ["python", "-u", "/opt/app/main.py"]
