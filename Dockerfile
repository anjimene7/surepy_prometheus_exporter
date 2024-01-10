FROM python:3.10-bullseye

WORKDIR /opt/app

ADD ./requirements.txt /opt/app/requirements.txt

RUN apt-get update -y \
    && apt-get upgrade -y \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install -r requirements.txt

#RUN sed -i 's#https://app.api.surehub.io/api#https://app-api.blue.production.surehub.io/api#g' /usr/local/lib/python3.10/site-packages/surepy/const.py \
#    && sed -i 's#app.api.surehub.io#app-api.blue.production.surehub.io#g' /usr/local/lib/python3.10/site-packages/surepy/client.py

ADD ./main.py /opt/app

CMD ["python", "-u", "/opt/app/main.py"]
