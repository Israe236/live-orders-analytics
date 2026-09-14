# Build context: frontends/ (the npm workspace root), so the shared @rad/core package is included.
FROM node:24-alpine AS build
WORKDIR /app
ENV NG_CLI_ANALYTICS=false

# npm ci checks the lock file against every workspace's package.json, so all of them are copied.
COPY package.json package-lock.json ./
COPY packages/core/package.json packages/core/
COPY web-react/package.json web-react/
COPY web-angular/package.json web-angular/
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund --workspace @rad/core --workspace @rad/web-angular

COPY packages/core packages/core
COPY web-angular web-angular
RUN npm run build -w @rad/core && npm run build -w @rad/web-angular

FROM nginx:1.29-alpine
COPY web-angular/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/web-angular/dist/web-angular/browser /usr/share/nginx/html
