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

const LAND_REGISTRY_API_BASE = process.env.LAND_REGISTRY_API_BASE || 'https://use-land-property-data.service.gov.uk/api/v1';
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

async function ensureTmpDir() {
  await fs.promises.mkdir(TMP_DIR, { recursive: true });
}

async function fetchMetadata(cfg: DatasetConfig): Promise<DatasetMetadata> {
  const url = new URL(`${LAND_REGISTRY_API_BASE.replace(/\/$/, '')}/${cfg.apiPath}`);
  if (LAND_REGISTRY_API_KEY) {
    url.searchParams.set('key', LAND_REGISTRY_API_KEY);
  }
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (LAND_REGISTRY_API_KEY) {
    headers['Authorization'] = `Bearer ${LAND_REGISTRY_API_KEY}`;
  }
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    throw new Error(`Failed to fetch metadata for ${cfg.apiPath}: ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<DatasetMetadata>;
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
  const csvEntry = directory.files.find(file => file.path.toLowerCase().endsWith('.csv'));
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

async function run() {
  await ensureTmpDir();
  const pool = new Pool({ connectionString: DATABASE_URL });
  const client = await pool.connect();

  try {
    for (const cfg of DATASETS) {
      try {
        console.log(`\nChecking updates for ${cfg.apiPath} (${cfg.label})...`);
        const meta = await fetchMetadata(cfg);
        const changeUrl = meta.latest_change_download_url || meta.latest_change || meta.change_download_url;
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
        console.error(`Failed to process updates for ${cfg.apiPath}:`, err);
      }
    }
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
