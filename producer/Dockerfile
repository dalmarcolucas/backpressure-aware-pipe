FROM node:24-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build:ts

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

EXPOSE 3000
CMD ["npx", "fastify", "start", "-a", "0.0.0.0", "-l", "info", "dist/app.js"]
