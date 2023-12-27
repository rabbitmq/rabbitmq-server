# syntax=docker/dockerfile:1
FROM atools/jdk-maven-node:mvn3-jdk11-node16 as base

WORKDIR /code

COPY package.json package.json
COPY run-amqp10-roundtriptest run-amqp10-roundtriptest

FROM base as test
RUN npm install
RUN mkdir -p /code/amqp10-roundtriptest
COPY amqp10-roundtriptest /code/amqp10-roundtriptest
RUN mvn -f /code/amqp10-roundtriptest package

ENTRYPOINT [ "npm" ]
CMD [ "" ]
