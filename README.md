# Express Postgres Starter

A minimal Express.js template with PostgreSQL connection, designed for Polsia infrastructure provisioning.

## Requirements

- Node.js 18+
- PostgreSQL database (Neon recommended)

## Environment Variables

- `DATABASE_URL` - PostgreSQL connection string (required)
- `PORT` - Server port (default: 3000)

## Endpoints

- `GET /` - Hello message
- `GET /health` - Health check (verifies database connection)

## Local Development

```bash
npm install
DATABASE_URL="postgresql://..." npm run dev
```

## Deployment

This template is configured for Render deployment via `render.yaml`.
