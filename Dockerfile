FROM node:20-alpine
WORKDIR /app

# Copy manifests (use wildcards only where valid)
COPY package.json ./
COPY package-lock.json* ./
COPY .npmrc* ./

# Install deps
RUN npm ci || npm i

# Copy the rest and build
COPY . .
RUN npm run build
RUN npm prune --omit=dev || true

EXPOSE 3000
ENV NODE_ENV=production

CMD ["npm", "run", "start"]
