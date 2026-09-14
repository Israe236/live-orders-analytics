# mobile-react-native

React Native (Expo SDK 57) version of the live orders dashboard: KPI grid, revenue sparkline,
category bars, orders by status, firing alerts and the live event feed. Connection handling and
state merging come from the shared [`@rad/core`](../packages/core) package; see
[docs/DECISIONS.md](../../docs/DECISIONS.md) for the design.

It is not part of `docker compose` (a phone app does not run in a container).

## Run on a phone with Expo Go

1. Start the backend: `docker compose up` at the repository root.
2. Find your computer's LAN IP (`ipconfig` on Windows) and create `.env` from `.env.example`:
   `EXPO_PUBLIC_API_URL=http://<your-lan-ip>:8000`. The phone and the computer must be on the same
   network, and the firewall must allow port 8000.
3. From `frontends/`: `npm install && npm run build:core`
4. `npm run start -w @rad/mobile`, then scan the QR code with Expo Go.

Other targets: Android emulator uses `http://10.0.2.2:8000`; iOS simulator and Expo web
(`npm run web -w @rad/mobile`) use `http://localhost:8000`.

## Checks

```bash
npm run typecheck -w @rad/mobile
npm run test -w @rad/mobile                            # sparkline path unit tests
npx expo export --platform web --output-dir dist       # proves the app bundles with Metro
```
