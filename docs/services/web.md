# Web Frontend

Purpose: React/Vite dashboard for managing strategies, providers, and monitoring performance.

## Run locally

- Install deps: `cd web && npm install`
- Dev server: `npm run dev` (defaults to port 3000)
- Build: `npm run build`, preview: `npm run preview`

## API access (dev)

- Proxy API requests to backend services; ensure CORS configured for `localhost:3000`.
- Set `.env` or Vite config for API base URLs if needed.

## Deployment

- `npm run build` outputs static assets in `web/dist` (or `web/build`).
- In production, Nginx serves static files and proxies API routes.

## Tests

- Add unit/component tests alongside `web/src` components as the frontend evolves.

## Notes

- Update architecture diagram if ports or proxy paths change.
- Keep UI docs in sync with backend capabilities.

