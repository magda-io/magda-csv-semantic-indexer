FROM node:24-alpine

EXPOSE 6305

RUN mkdir -p /usr/src/app
COPY . /usr/src/app
WORKDIR /usr/src/app/component
ENTRYPOINT [ "node", "/usr/src/app/component/dist/index.js" ]