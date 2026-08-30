# Bifrost server image: the static site is built first, the server compiled second, both copied into a small runtime.
FROM node:22-bookworm-slim AS web
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM node:22-bookworm-slim AS server
WORKDIR /app/server
COPY server/package.json server/package-lock.json ./
RUN npm ci
COPY server/tsconfig.json ./
COPY server/src ./src
RUN npm run build && npm prune --omit=dev

FROM node:22-bookworm-slim
WORKDIR /app/server
ENV NODE_ENV=production
COPY --from=server /app/server/node_modules ./node_modules
COPY --from=server /app/server/dist ./dist
COPY --from=server /app/server/package.json ./
COPY --from=web /app/server/public ./public
# CLI binaries built by CI land in bin/ (mounted at run time from the deploy directory).
USER 1600:1600
EXPOSE 8080
CMD ["node", "dist/index.js"]
