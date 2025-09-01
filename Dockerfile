# Simple Node build
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* .npmrc* ./ 2>/dev/null || true
RUN npm i
COPY . .
RUN npm run build
