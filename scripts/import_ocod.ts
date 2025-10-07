// scripts/import_ocod.ts
import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { Pool } from 'pg';

const DATABASE_URL = process.env.DATABASE_URL || '';
if (!DATABASE_URL) {
  console.error('Set DATABASE_URL (Postgres/Neon connection string)');
  process.exit(1);
}

const FILE_PATH = process.argv[2];
if (!FILE_PATH) {
  console.error('Usage: node --loader ts-node/esm scripts/import_ocod.ts ./reference/OCOD_FULL_2025_10.csv intl');
  process.exit(1);
}

const DATASET_LABEL = process.argv[3] || 'intl';  // OCOD by default
const BATCH_SIZE = 1000;

const pool = new Pool({ connectionString: DATABASE_URL });

type RawRecord = Record<string, string>;

function normaliseAddressKey(propertyAddress: string, postcode: string) {
  const addr = (propertyAddress || '').trim().toLowerCase().replace(/\s+/g, ' ');
  const pc = (postcode || '').replace(/\s+/g, '').toLowerCase();
  return `${addr}|${pc}`;
}

function buildOwnerRows(row: RawRecord) {
  const owners: Array<{ owner_name: string; company_number: string | null; raw: any }> = [];

  const proprietorSets: Array<[string, string, string, string, string, string, string]> = [
    [row['Proprietor Name (1)'], row['Company Registration No. (1)'], row['Proprietorship Category (1)'],
     row['Proprietor (1) Address (1)'], row['Proprietor (1) Address (2)'], row['Proprietor (1) Address (3)'],
     row['Country Incorporated (1)']],
    [row['Proprietor Name (2)'], row['Company Registration No. (2)'], row['Proprietorship Category (2)'],
     row['Proprietor (2) Address (1)'], row['Proprietor (2) Address (2)'], row['Proprietor (2) Address (3)'],
     row['Country Incorporated (2)']],
    [row['Proprietor Name (3)'], row['Company Registration No. (3)'], row['Proprietorship Category (3)'],
     row['Proprietor (3) Address (1)'], row['Proprietor (3) Address (2)'], row['Proprietor (3) Address (3)'],
     row['Country Incorporated (3)']],
    [row['Proprietor Name (4)'], row['Company Registration No. (4)'], row['Proprietorship Category (4)'],
     row['Proprietor (4) Address (1)'], row['Proprietor (4) Address (2)'], row['Proprietor (4) Address (3)'],
     row['Country Incorporated (4)']],
  ];

  proprietorSets.forEach(([name, regNo, category, addr1, addr2, addr3, country]) => {
    const trimmedName = (name || '').trim();
    if (!trimmedName) return;

    const cleanReg = (regNo || '').replace(/\s+/g, '').trim();
    owners.push({
      owner_name: trimmedName,
      company_number: cleanReg || null,
      raw: {
        name: trimmedName,
        company_number: cleanReg || null,
        category: (category || '').trim() || null,
        address_lines: [addr1, addr2, addr3].filter(Boolean),
        country_incorporated: (country || '').trim() || null,
      },
    });
  });

  return owners;
}

async function run() {
  const client = await pool.connect();
  try {
    const start = Date.now();
    console.log(`Starting OCOD import (${DATASET_LABEL}) from ${FILE_PATH}`);
    await client.query('BEGIN');
    await client.query('DELETE FROM land_registry_corporate WHERE dataset_label = $1', [DATASET_LABEL]);

    const stream = fs.createReadStream(path.resolve(FILE_PATH)).pipe(csv());
    const batch: any[] = [];
    let total = 0;
    let properties = 0;

    for await (const row of stream) {
      if (row['Title Number'] && row['Title Number'].startsWith('Row Count')) {
        continue; // skip summary lines
      }

      const owners = buildOwnerRows(row);
      if (!owners.length) continue;

      const addressKey = normaliseAddressKey(row['Property Address'] || '', row['Postcode'] || '');
      if (!addressKey.trim()) continue;

      properties += 1;

      owners.forEach((owner) => {
        batch.push({
          address_key: addressKey,
          owner_name: owner.owner_name,
          company_number: owner.company_number,
          dataset_label: DATASET_LABEL,
          raw: {
            title_number: row['Title Number'] || null,
            tenure: row['Tenure'] || null,
            property_address: row['Property Address'] || null,
            district: row['District'] || null,
            county: row['County'] || null,
            region: row['Region'] || null,
            postcode: row['Postcode'] || null,
            price_paid: row['Price Paid'] || null,
            proprietor: owner.raw,
            date_proprietor_added: row['Date Proprietor Added'] || null,
            additional_proprietor_indicator: row['Additional Proprietor Indicator'] || null,
          },
        });
      });

      if (batch.length >= BATCH_SIZE) {
        await insertBatch(client, batch);
        total += batch.length;
        batch.length = 0;
        if (total % 20000 === 0) {
          console.log(`Inserted ${total.toLocaleString()} owner rows so far...`);
        }
        if (properties % 5000 === 0) {
          console.log(`Processed ${properties.toLocaleString()} properties...`);
        }
      }
    }

    if (batch.length) {
      await insertBatch(client, batch);
      total += batch.length;
    }

    await client.query(
      `INSERT INTO land_registry_corporate_meta (dataset_label, last_refreshed_at, source_url, row_count)
       VALUES ($1, now(), $2, $3)
       ON CONFLICT (dataset_label)
         DO UPDATE SET last_refreshed_at = EXCLUDED.last_refreshed_at,
                       source_url = EXCLUDED.source_url,
                       row_count = EXCLUDED.row_count`,
      [DATASET_LABEL, FILE_PATH, total],
    );

    await client.query('CREATE INDEX IF NOT EXISTS land_registry_corporate_address_key_idx ON land_registry_corporate(address_key)');
    await client.query('COMMIT');
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`Imported ${total.toLocaleString()} owner rows across ${properties.toLocaleString()} properties in ${elapsed}s.`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Import failed:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

async function insertBatch(client: any, batch: any[]) {
  const insertValues: string[] = [];
  const params: any[] = [];

  batch.forEach((item, index) => {
    const base = index * 5;
    insertValues.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
    params.push(
      item.address_key,
      item.owner_name,
      item.company_number ?? null,
      item.dataset_label,
      JSON.stringify(item.raw)
    );
  });

  if (!insertValues.length) return;

  await client.query(
    `INSERT INTO land_registry_corporate (address_key, owner_name, company_number, dataset_label, raw)
     VALUES ${insertValues.join(',')}
     ON CONFLICT (address_key, owner_name, dataset_label) DO NOTHING`,
    params
  );
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
