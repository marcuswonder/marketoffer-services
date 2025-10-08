import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';
import { spawn } from 'child_process';
import { Pool } from 'pg';
import unzipper from 'unzipper';
import { fetch } from 'undici';

const DATABASE_URL = process.env.DATABASE_URL || '';
if (!DATABASE_URL) {
  console.error('Set DATABASE_URL before running (postgres://...)');
  process.exit(1);
}

function normalizeBase(input?: string) {
  const raw = (input || '').trim();
  const base = raw || 'https://use-land-property-data.service.gov.uk/api/v1';
  const noTrailing = base.replace(/\/$/, '');
  // Ensure we always target /api/v1
  if (/\/api\/v1$/.test(noTrailing)) return noTrailing;
  if (/\/api$/.test(noTrailing)) return `${noTrailing}/v1`;
  return noTrailing.endsWith('/v1') ? noTrailing : `${noTrailing}/api/v1`;
}

const API_BASE = normalizeBase(process.env.LAND_REGISTRY_API_BASE);
const DATASET_BASE = `${API_BASE}/datasets`;
const LAND_REGISTRY_API_KEY = process.env.LAND_REGISTRY_API_KEY || '';
const TMP_DIR = path.join(process.cwd(), 'tmp', 'land-registry');

const DATASETS = [
  { apiPath: 'ccod', label: 'uk', updateScript: 'scripts/update_ccod.ts' },
  { apiPath: 'ocod', label: 'intl', updateScript: 'scripts/update_ocod.ts' },
] as const;

type DatasetConfig = typeof DATASETS[number];

type DatasetMetadata = {
  latest_full_download_url?: string;
  latest_change_download_url?: string;
  [key: string]: any;
};

async function fetchChangeDownloadUrl(cfg: DatasetConfig, resource?: any): Promise<string | null> {
  if (!resource) return null;

  // const direct = resource.latest?.download_url || resource.download_url;
  // if (direct) return direct;

  const fileName = resource.file_name;
  if (!fileName) return null;

  const url = new URL(`${DATASET_BASE}/${cfg.apiPath}/${fileName}`);
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (LAND_REGISTRY_API_KEY) {
    headers['Authorization'] = LAND_REGISTRY_API_KEY;
  }

  console.log(`Fetching resource metadata from ${url.toString()}`);

  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    throw new Error(`Failed to fetch resource metadata for ${cfg.apiPath}/${fileName}: ${res.status} ${res.statusText}`);
  }

  const text = await res.text();
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    console.log(`Non-JSON response for ${cfg.apiPath}/${fileName}. Snippet:`, text.slice(0, 1000));
    return null;
  }

  if (data && typeof data === 'object') {
    console.log(
      `Resource metadata keys for ${cfg.apiPath}:`,
      Object.keys(data as Record<string, unknown>)
    );
    console.log(
      `Full resource metadata for ${cfg.apiPath}:`,
      JSON.stringify(data, null, 2)
    );
  } else {
    console.log(`Parsed non-object JSON for ${cfg.apiPath}/${fileName}:`, String(data));
  }

  const anyData = data as any;
  const downloadUrl =
    anyData?.result?.latest?.download_url ??
    anyData?.result?.download_url ??
    anyData?.latest?.download_url ??
    anyData?.download_url ??
    null;

  return typeof downloadUrl === 'string' ? downloadUrl : null;

}

async function ensureTmpDir() {
  await fs.promises.mkdir(TMP_DIR, { recursive: true });
}

async function fetchMetadata(cfg: DatasetConfig): Promise<DatasetMetadata> {
  const url = new URL(`${DATASET_BASE}/${cfg.apiPath}`);

  const headers: Record<string, string> = { Accept: 'application/json' };
  if (LAND_REGISTRY_API_KEY) {
    headers['Authorization'] = LAND_REGISTRY_API_KEY;
  }

  console.log(`Fetching metadata from ${url.toString()}`);
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    throw new Error(`Failed to fetch metadata for ${cfg.apiPath}: ${res.status} ${res.statusText}`);
  }
  console.log(`Fetched metadata for ${cfg.apiPath}: ${res.status} ${res.statusText}`);

  const data = (await res.json()) as unknown;
    if (data && typeof data === 'object') {
    console.log(
      `Metadata keys for ${cfg.apiPath}:`,
      Object.keys(data as Record<string, unknown>)
    );
    console.log(
      `Full metadata sample for ${cfg.apiPath}:`,
      JSON.stringify(data, null, 2)
    );
  } else {
    console.log(`Non-object response for ${cfg.apiPath}:`, String(data));
  }

  return data as DatasetMetadata;
}

async function downloadFile(url: string, cfg: DatasetConfig): Promise<string> {
  const res = await fetch(url);
  if (!res.ok || !res.body) {
    throw new Error(`Failed to download ${cfg.apiPath} change file: ${res.status} ${res.statusText}`);
  }
  const ext = path.extname(new URL(url).pathname) || '.csv';
  const targetPath = path.join(TMP_DIR, `${cfg.apiPath}-changes-${Date.now()}${ext}`);
  await pipeline(res.body, fs.createWriteStream(targetPath));
  return targetPath;
}

async function extractZip(zipPath: string): Promise<string> {
  const directory = await unzipper.Open.file(zipPath);
  const csvEntry = directory.files.find((file: any) => file.path.toLowerCase().endsWith('.csv'));
  if (!csvEntry) {
    throw new Error(`Zip ${zipPath} does not contain a CSV file`);
  }
  const extractedPath = path.join(TMP_DIR, csvEntry.path);
  await fs.promises.mkdir(path.dirname(extractedPath), { recursive: true });
  await pipeline(csvEntry.stream(), fs.createWriteStream(extractedPath));
  return extractedPath;
}

async function runUpdateScript(scriptPath: string, csvPath: string, label: string, sourceUrl: string) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(process.execPath, ['--loader', 'ts-node/esm', scriptPath, csvPath, label, sourceUrl], {
      stdio: 'inherit',
    });
    child.on('exit', code => {
      if (code === 0) resolve();
      else reject(new Error(`${path.basename(scriptPath)} exited with code ${code}`));
    });
    child.on('error', reject);
  });
}

function formatError(err: unknown): string {
  if (err instanceof Error) return err.stack || err.message;
  try { return JSON.stringify(err); } catch { return String(err); }
}

async function run() {
  await ensureTmpDir();
  const pool = new Pool({ connectionString: DATABASE_URL });
  const client = await pool.connect();

  console.log(`Using API_BASE: ${API_BASE}`);

  try {
    for (const cfg of DATASETS) {
      try {
        console.log(`\nChecking updates for ${cfg.apiPath} (${cfg.label})...`);
        const meta = await fetchMetadata(cfg);

        const resources = meta.result?.resources ?? [];
        console.log(`Resources found for ${cfg.apiPath}: `, resources)

        const changeResource = resources.find((r: any) => {
          const name = (r.name || '').toLowerCase();
          return name === 'change only file' || name.includes('change only');
        });
        console.log(`Change Resource Found: ${changeResource ? 'yes' : 'no'}`);

        const changeUrl = await fetchChangeDownloadUrl(cfg, changeResource);
        if (!changeUrl) {
          console.log(`No change download URL found for ${cfg.apiPath}; skipping.`);
          continue;
        }

        const { rows } = await client.query<{ source_url: string | null }>(
          'SELECT source_url FROM land_registry_corporate_meta WHERE dataset_label = $1 LIMIT 1',
          [cfg.label]
        );
        const lastSource = rows[0]?.source_url || null;
        if (lastSource === changeUrl) {
          console.log(`Already up to date for ${cfg.label} (${changeUrl}).`);
          continue;
        }

        console.log(`New change file detected for ${cfg.label}: ${changeUrl}`);
        const downloaded = await downloadFile(changeUrl, cfg);
        let csvPath = downloaded;
        if (downloaded.toLowerCase().endsWith('.zip')) {
          console.log(`Extracting ${downloaded}...`);
          csvPath = await extractZip(downloaded);
        }

        console.log(`Applying changes from ${csvPath}...`);
        await runUpdateScript(cfg.updateScript, csvPath, cfg.label, changeUrl);

        // Clean up temporary files to save disk space
        await fs.promises.rm(csvPath).catch(() => {});
        if (downloaded !== csvPath) {
          await fs.promises.rm(downloaded).catch(() => {});
        }
      } catch (err) {
        console.error(`Failed to process updates for ${cfg.apiPath}: ${formatError(err)}`);
      }
    }
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(err => {
  console.error('Land Registry update failed:', formatError(err));
  process.exit(1);
});
