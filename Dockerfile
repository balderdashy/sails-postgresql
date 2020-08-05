FROM node:lts-alpine

ADD package.json package.json
ADD yarn.lock yarn.lock
RUN yarn || (npm install -g yarn && yarn install)
ADD . .
