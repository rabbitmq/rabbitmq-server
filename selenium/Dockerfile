FROM node:18 AS base

WORKDIR /code

COPY package.json package.json

FROM base AS test
RUN npm install --verbose

ENTRYPOINT [ "npm" ]
CMD [ "" ]
