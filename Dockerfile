FROM node:20-alpine
WORKDIR /app

# Copy manifests (use wildcards only where valid)
COPY package.json ./
COPY package-lock.json* ./
COPY .npmrc* ./

# Install (use ci if you have a lockfile)
RUN npm ci || npm i

# Copy the rest and build
COPY . .
RUN npm run build