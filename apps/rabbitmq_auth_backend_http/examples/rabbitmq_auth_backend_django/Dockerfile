FROM python:3.7-alpine

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /usr/src/app

RUN pip install django       


COPY . /usr/src/app

EXPOSE 8000

ENTRYPOINT ["/usr/src/app/start.sh"]
