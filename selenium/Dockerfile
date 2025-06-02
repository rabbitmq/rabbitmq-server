FROM node:18 as base

WORKDIR /code

COPY package.json package.json

FROM base as test
RUN npm install

ENTRYPOINT [ "npm" ]
CMD [ "" ]
