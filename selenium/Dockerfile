# syntax=docker/dockerfile:1
FROM atools/jdk-maven-node:mvn3-jdk11-node16 as base

WORKDIR /code

COPY package.json package.json

FROM base as test
RUN npm install

ENTRYPOINT [ "npm" ]
CMD [ "" ]
