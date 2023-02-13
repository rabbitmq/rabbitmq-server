# syntax=docker/dockerfile:1
FROM node:16.19.0 as base

WORKDIR /code

COPY package.json package.json

FROM base as test
RUN npm install
ENTRYPOINT [ "npm" ]
CMD [ "" ]
