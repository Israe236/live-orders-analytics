# web-angular

Angular 22 version of the live orders dashboard (standalone components, zoneless change detection,
signals, OnPush, tree-shaken ECharts via `ngx-echarts`). Live data and reconnection logic come from
the shared [`@rad/core`](../packages/core) package. See the [project README](../../README.md) and
[docs/DECISIONS.md](../../docs/DECISIONS.md) for the design.

```bash
# from frontends/
npm install
npm run build:core
npm run start -w @rad/web-angular   # http://localhost:4200, proxies /api and /ws to localhost:8000
npm run test -w @rad/web-angular    # vitest via the Angular unit-test builder
npm run lint -w @rad/web-angular
npm run build -w @rad/web-angular
```
