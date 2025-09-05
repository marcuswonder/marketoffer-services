# MarketOffer Services (API, Workers, Redis, Postgres, pgAdmin)

Local development stack for MarketOffer scraping/processing pipelines.

What’s included
- **API**: Node + Express (`src/api/index.ts`) with Bull Board.
- **Workers**: TypeScript workers for 3 pipelines (CH appointments, company discovery, person LinkedIn).
- **Queues**: Redis + BullMQ (`src/queues`).
- **Postgres**: job tracking + CH data storage (`src/lib/progress.ts`).
- **pgAdmin**: UI on port 5050 with auto‑registered server.
- **Docker Compose**: profiles to run infra/app/workers independently.

## Prerequisites
- Docker + Docker Compose
- Node 18+ (for local dev scripts)
- `.env` populated (already present in this repo for local use)

## Running locally

Common commands
- Infra only (Redis, Postgres, pgAdmin): `docker compose up -d`
- API only: `docker compose --profile app up -d web`
- Workers only: `docker compose --profile workers up -d`
- API + Workers: `docker compose --profile app --profile workers up -d`
- Stop everything: `docker compose down`

Ports
- API: `http://localhost:3000`
- Bull Board: `http://localhost:3000/admin/queues`
- pgAdmin: `http://localhost:5050`

## pgAdmin auto‑registration
- A server named “MarketOffer Postgres” is preconfigured via `pgadmin/servers.json` and mounted into the container.
- On first start, log in with `.env` credentials (`PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`).
- If pgAdmin has run before, the import won’t reapply. To reset:
  - `docker compose down`
  - `docker volume rm marketoffer_services_pgadmin_data`
  - `docker compose up -d pgadmin`

## Database
- Postgres is seeded on demand (no schema file). The API and workers run `initDb()` to create required tables (`job_progress`, `job_events`, `ch_people`, `ch_appointments`).
- Connection uses `DATABASE_URL` if set; otherwise it is built from `POSTGRES_HOST/PORT/USER/PASSWORD/DB` (see `src/lib/db.ts`).
- For Node running outside Docker, set `POSTGRES_HOST=localhost` (or set `DATABASE_URL` accordingly). Inside Compose, `POSTGRES_HOST=postgres` works via the Docker network.

## API endpoints
- Health: `GET /health`
- Create CH appointments job: `POST /api/jobs/ch-appointments` with JSON `{ companyNumber, firstName?, lastName?, contactId? }`
- Create company discovery job: `POST /api/jobs/company-discovery` with JSON `{ companyNumber?, companyName?, address?, postcode? }`
- Create person LinkedIn job: `POST /api/jobs/person-linkedin` with JSON `{ personId }`
- Job progress list: `GET /api/progress/jobs?limit=50&queue=...&status=...`
- Job detail: `GET /api/progress/jobs/:jobId`

Example
```bash
curl -X POST http://localhost:3000/api/jobs/ch-appointments \
  -H 'content-type: application/json' \
  -d '{"companyNumber":"01234567","firstName":"Jane","lastName":"Doe"}'
```

## Local development (without Docker for app)
- Ensure Redis/Postgres are running (e.g., `docker compose up -d redis postgres`).
- Update `.env` so `POSTGRES_HOST=localhost` and `REDIS_URL=redis://localhost:6379`.
- Run API: `npm run dev`
- Run workers (pick one):
  - `npm run dev:worker:ch`
  - `npm run dev:worker:company`
  - `npm run dev:worker:person`

## Volumes (useful during resets)
- List all: `docker volume ls`
- List project volumes: `docker volume ls -f "label=com.docker.compose.project=marketoffer_services"`
- Remove pgAdmin data: `docker volume rm marketoffer_services_pgadmin_data`
- Remove Postgres data (destroys DB): `docker volume rm marketoffer_services_pgdata`
- Remove all unused: `docker volume prune`

## Project structure
```
.
├─ src/
│  ├─ api/
│  │  ├─ index.ts            # Express app + Bull Board
│  │  └─ routes/
│  │     ├─ jobs.ts          # enqueue endpoints
│  │     └─ progress.ts      # job progress/events API
│  ├─ queues/                # BullMQ connection + queues
│  ├─ workers/               # CH / Company / Person workers
│  └─ lib/                   # db, http, logger, etc.
├─ pgadmin/servers.json      # auto‑register pg connection
├─ docker-compose.yml        # services + profiles
├─ Dockerfile                # Node 20 build image
├─ package.json              # scripts
└─ .env                      # local configuration
```

## Notes
- Feature flags: `WRITE_TO_AIRTABLE=false` by default; set true to write via `src/lib/airtable.ts`.
- Rate limiting and external API keys are read from `.env`.
- Be careful with `down -v` and volume removals—they are irreversible.
