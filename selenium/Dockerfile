FROM node:24 AS base

WORKDIR /code

COPY package.json package.json
COPY .npmrc ./

FROM base AS test
RUN npm install --ignore-scripts --verbose

ENTRYPOINT [ "npm" ]
CMD [ "" ]
