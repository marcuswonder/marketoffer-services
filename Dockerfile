# Use an official Node.js 20 Alpine image
FROM node:20-alpine AS builder

# Set working directory
WORKDIR /app

# Copy dependency manifests first (for caching)
COPY package.json package-lock.json* ./
COPY .npmrc* ./

# Install dependencies (ci preferred, fallback to install)
RUN npm ci || npm i

# Copy application source and build
COPY . .
RUN npm run build

# ---- Production image ----
FROM node:20-alpine
WORKDIR /app

# Copy only built output and production deps from builder stage
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

# Environment setup
ENV NODE_ENV=production
EXPOSE 3000

# Default command (can be overridden in Railway)
CMD ["npm", "run", "start"]