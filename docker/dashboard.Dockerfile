FROM node:22-bookworm-slim

WORKDIR /app
COPY . .
RUN npm install -g corepack@latest && corepack pnpm install && corepack pnpm run build

ENV NODE_ENV=production
EXPOSE 3000
CMD ["node", "dist/index.js"]
