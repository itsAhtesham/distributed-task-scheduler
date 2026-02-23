FROM node:20-alpine AS base
WORKDIR /app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm ci --production=false

# Copy source
COPY tsconfig.json ./
COPY src ./src

# Production stage
FROM base AS production
ENV NODE_ENV=production
EXPOSE 3000

# Default command (overridden by docker-compose)
CMD ["npx", "tsx", "src/index.ts"]
