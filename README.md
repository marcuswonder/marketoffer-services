# MarketOffer Services (API, Workers, Redis)

Local development stack for MarketOffer scraping/processing pipelines.

What’s included
- **API**: Node + Express (`src/api/index.ts`) with Bull Board.
- **Workers**: TypeScript workers for 3 pipelines (CH appointments, company discovery, person LinkedIn).
- **Queues**: Redis + BullMQ (`src/queues`).
- **Postgres (external)**: job tracking + CH data storage (`src/lib/progress.ts`).
- **Docker Compose**: profiles to run infra/app/workers independently (Redis is the only bundled service).

## Prerequisites
- Docker + Docker Compose
- Node 18+ (for local dev scripts)
- `.env` populated (already present in this repo for local use)

## Running locally

Common commands
- Infra only (Redis): `docker compose up -d redis`
- API only: `docker compose --profile app up -d web`
- Workers only: `docker compose --profile workers up -d`
- API + Workers: `docker compose --profile app --profile workers up -d`
- Stop everything: `docker compose down`

Ports
- API: `http://localhost:3000`
- Bull Board: `http://localhost:3000/admin/queues`

## Database
- Postgres is seeded on demand (no schema file). The API and workers run `initDb()` to create required tables (`job_progress`, `job_events`, `ch_people`, `ch_appointments`).
- Connection uses `DATABASE_URL` if set; otherwise it is built from `POSTGRES_HOST/PORT/USER/PASSWORD/DB` (see `src/lib/db.ts`).
- The stack now expects an external Postgres (e.g. Neon). When running locally, set `POSTGRES_HOST`/`DATABASE_URL` to point at your hosted instance. Redis is the only stateful service in Docker Compose.

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

- Ensure Redis is running (e.g., `docker compose up -d redis`) and that `DATABASE_URL` points to your external Postgres.
- Update `.env` so `POSTGRES_HOST=localhost` and `REDIS_URL=redis://localhost:6379`.
- Run API: `npm run dev`
- Run workers (pick one):
  - `npm run dev:worker:ch`
  - `npm run dev:worker:company`
- `npm run dev:worker:person`

## Owner discovery workflow

The `owner-discovery` worker now produces a single canonical log stream per request. When a job includes `rootJobId`, every downstream action (Companies House, company discovery, LinkedIn enrichment) mirrors its telemetry into that root job so `/requests/:id` shows the full trail. Outcomes are resolved as:

- **Corporate owner found**: Land Registry match sets the property status to `corporate`, then enqueues a `ch-appointments` fetch (with propagated `rootJobId`). The CH worker scrapes directors/PSCs, persists them, triggers company discovery for each active appointment, and now spins up LinkedIn enrichment jobs per director.
- **Open Register occupant present, no determination, not a director**: No queues are dispatched. The worker logs the scored candidates and the reasons evidence fell short so the request UI shows why no strong owner was recorded.
- **Open Register occupant present, no determination, director**: As above, but the log includes the Companies House overlap so the reviewer knows which corporate links were found even though ownership confidence stayed below the accept threshold.
- **Open Register owner confirmed, not a director**: The worker enqueues a `person-linkedin` job seeded with the Open Register data (name, tenure, address context). Results land back on the original owner job detail.
- **Open Register owner confirmed, director**: The worker enqueues the targeted `ch-appointments` fetch for that director (respecting `OWNER_QUEUE_CH_DIRECTORS`) and also schedules LinkedIn enrichment. The CH worker downstream performs company discovery for each appointment and forwards LinkedIn lookups for the director profile.
- **No open data**: When neither corporate nor Open Register/CH signals exist, the job resolves to `no_public_data` and logs that outcome at both the child and root job ids.

## Volumes (useful during resets)
- List all: `docker volume ls`
- List project volumes: `docker volume ls -f "label=com.docker.compose.project=marketoffer_services"`
- Remove Redis data: `docker volume rm marketoffer_services_redis-data` (if you add a volume mapping)
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
