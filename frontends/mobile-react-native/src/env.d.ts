declare namespace NodeJS {
  interface ProcessEnv {
    /** Base URL of the API, e.g. http://192.168.1.10:8000 (inlined by Expo at bundle time). */
    readonly EXPO_PUBLIC_API_URL?: string;
  }
}
