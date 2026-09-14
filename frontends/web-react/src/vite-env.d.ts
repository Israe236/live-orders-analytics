/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** REST base path or URL. Defaults to "/api" (proxied to the API by Vite or nginx). */
  readonly VITE_API_BASE?: string;
  /** Full WebSocket URL. Defaults to "/ws/live" on the page's own host. */
  readonly VITE_WS_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
