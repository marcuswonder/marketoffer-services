# MEN (+Redis) Scraper Starter

Production-ready scaffold for your LinkedIn/Company discovery pipelines using:
- **Node + Express** (API)
- **Redis + BullMQ** (queues, retries, concurrency)
- **Workers** (TypeScript) for 3 pipelines:
  1. Companies House Appointments
  2. Company Website/LinkedIn Discovery
  3. Person (Director) LinkedIn Discovery
- **Airtable** as the user-facing store
- **Docker Compose** for local dev (web, multiple workers, Redis)

## Quick start

```bash
# 1) Copy env file and fill in secrets
cp .env.example .env

# 2) Install deps
npm i

# 3) Run locally (requires Docker)
docker compose up --build
# Web API: http://localhost:3000
# Bull Board: http://localhost:3000/admin/queues
```

## Deploy targets
- Render / Railway / Fly.io (one "web" service, one or more "worker" services, Redis add-on)
- Or self-host on a VM with Docker Compose.

## Structure
```
.
├─ src/
│  ├─ api/
│  │  ├─ index.ts            # Express app
│  │  └─ routes/
│  │     └─ jobs.ts
│  ├─ queues/
│  │  └─ index.ts            # BullMQ queues/schedulers
│  ├─ workers/
│  │  ├─ chAppointments.ts
│  │  ├─ companyDiscovery.ts
│  │  └─ personLinkedin.ts
│  └─ lib/
│     ├─ airtable.ts         # batch write helpers
│     ├─ http.ts             # fetch with retries/backoff
│     ├─ normalize.ts        # url & name cleaners
│     ├─ rateLimiter.ts      # Redis token buckets
│     └─ logger.ts           # pino (+ pretty in dev)
├─ config/
│  ├─ env.ts
│  └─ genericHosts.json      # directories to exclude
├─ scripts/
│  ├─ seedGenericHosts.ts    # load CSV → JSON list
│  └─ testAirtable.ts        # simple connectivity check
├─ airtable_schema/
│  ├─ People.csv
│  ├─ Companies.csv
│  ├─ Appointments.csv
│  └─ Jobs.csv
├─ docker-compose.yml
├─ Dockerfile
├─ package.json
├─ tsconfig.json
├─ .env.example
└─ README.md
```

## Airtable advice
- Use the CSVs in `/airtable_schema` to create your tables.
- Keep **operational** state (locks, checkpoints, rate tokens) in Redis, not Airtable.
- Batch writes in groups of 10 records.

## Rate limiting
Configure per-provider RPS in `.env`. The HTTP wrapper backs off on 429/5xx with exponential retry.

## Security & compliance
Stay within ToS. Store only public/consented URLs, log consent, and implement deletion flows.
